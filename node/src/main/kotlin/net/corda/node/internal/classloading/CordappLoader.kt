package net.corda.node.internal.classloading

import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner
import io.github.lukehutch.fastclasspathscanner.scanner.ScanResult
import net.corda.core.flows.ContractUpgradeFlow
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.InitiatedBy
import net.corda.core.flows.StartableByRPC
import net.corda.core.internal.div
import net.corda.core.internal.exists
import net.corda.core.internal.isRegularFile
import net.corda.core.internal.list
import net.corda.core.node.NodeInfo
import net.corda.core.node.services.CordaService
import net.corda.core.node.services.ServiceType
import net.corda.core.serialization.SerializeAsToken
import net.corda.core.utilities.debug
import net.corda.core.utilities.loggerFor
import java.lang.reflect.Modifier
import java.net.JarURLConnection
import java.net.URI
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*
import java.util.stream.Collectors
import kotlin.reflect.KClass

/**
 * Handles Cordapp loading and classpath scanning
 */
class CordappLoader(private val baseDir: Path, private val devMode: Boolean) {
    lateinit var appClassLoader: AppClassLoader
    val scanResult = scanCordapps()

    private companion object {
        val logger = loggerFor<CordappLoader>()
    }

    fun findServices(info: NodeInfo): List<Class<out SerializeAsToken>> {
        fun getServiceType(clazz: Class<*>): ServiceType? {
            return try {
                clazz.getField("type").get(null) as ServiceType
            } catch (e: NoSuchFieldException) {
                logger.warn("${clazz.name} does not have a type field, optimistically proceeding with install.")
                null
            }
        }

        return scanResult?.getClassesWithAnnotation(SerializeAsToken::class, CordaService::class)
                ?.filter {
                    val serviceType = getServiceType(it)
                    if (serviceType != null && info.serviceIdentities(serviceType).isEmpty()) {
                        logger.debug {
                            "Ignoring ${it.name} as a Corda service since $serviceType is not one of our " +
                                    "advertised services"
                        }
                        false
                    } else {
                        true
                    }
                } ?: emptyList<Class<SerializeAsToken>>()
    }

    fun findInitiatedFlows(): List<Class<out FlowLogic<*>>> {
        return scanResult?.getClassesWithAnnotation(FlowLogic::class, InitiatedBy::class)
                // First group by the initiating flow class in case there are multiple mappings
                ?.groupBy { it.requireAnnotation<InitiatedBy>().value.java }
                ?.map { (initiatingFlow, initiatedFlows) ->
                    val sorted = initiatedFlows.sortedWith(FlowTypeHierarchyComparator(initiatingFlow))
                    if (sorted.size > 1) {
                        logger.warn("${initiatingFlow.name} has been specified as the inititating flow by multiple flows " +
                                "in the same type hierarchy: ${sorted.joinToString { it.name }}. Choosing the most " +
                                "specific sub-type for registration: ${sorted[0].name}.")
                    }
                    sorted[0]
                } ?: emptyList<Class<out FlowLogic<*>>>()
    }

    fun findRPCFlows(): List<Class<out FlowLogic<*>>> {
        fun Class<out FlowLogic<*>>.isUserInvokable(): Boolean {
            return Modifier.isPublic(modifiers) && !isLocalClass && !isAnonymousClass && (!isMemberClass || Modifier.isStatic(modifiers))
        }

        val found = scanResult?.getClassesWithAnnotation(FlowLogic::class, StartableByRPC::class)?.filter { it.isUserInvokable() } ?: emptyList<Class<out FlowLogic<*>>>()
        val coreFlows = listOf(ContractUpgradeFlow.Initiator::class.java)
        return found + coreFlows
    }

    private fun scanCordapps(): ScanResult? {
        val scanPackage = System.getProperty("net.corda.node.cordapp.scan.package")
        val paths = if (scanPackage != null) {
            // Rather than looking in the plugins directory, figure out the classpath for the given package and scan that
            // instead. This is used in tests where we avoid having to package stuff up in jars and then having to move
            // them to the plugins directory for each node.
            check(devMode) { "Package scanning can only occur in dev mode" }
            val resource = scanPackage.replace('.', '/')
            javaClass.classLoader.getResources(resource)
                    .asSequence()
                    .map {
                        println(it)
                        val uri = if (it.protocol == "jar") {
                            (it.openConnection() as JarURLConnection).jarFileURL.toURI()
                        } else {
                            URI(it.toExternalForm().removeSuffix(resource))
                        }
                        Paths.get(uri)
                    }
                    .toList()
        } else {
            val pluginsDir = baseDir / "plugins"
            if (!pluginsDir.exists()) return null
            pluginsDir.list {
                it.filter { it.isRegularFile() && it.toString().endsWith(".jar") }.collect(Collectors.toList())
            }
        }

        logger.info("Scanning CorDapps in $paths")

        appClassLoader = AppClassLoader(1, paths.map { it.toUri().toURL() }.toTypedArray())

        println("=============================")
        println(appClassLoader.hashCode())
        println("=============================")

        // This will only scan the plugin jars and nothing else
        return if (paths.isNotEmpty()) FastClasspathScanner().addClassLoader(appClassLoader).overrideClasspath(paths).scan() else null
    }

    private class FlowTypeHierarchyComparator(val initiatingFlow: Class<out FlowLogic<*>>) : Comparator<Class<out FlowLogic<*>>> {
        override fun compare(o1: Class<out FlowLogic<*>>, o2: Class<out FlowLogic<*>>): Int {
            return if (o1 == o2) {
                0
            } else if (o1.isAssignableFrom(o2)) {
                1
            } else if (o2.isAssignableFrom(o1)) {
                -1
            } else {
                throw IllegalArgumentException("${initiatingFlow.name} has been specified as the initiating flow by " +
                        "both ${o1.name} and ${o2.name}")
            }
        }
    }

    private fun <T : Any> ScanResult.getClassesWithAnnotation(type: KClass<T>, annotation: KClass<out Annotation>): List<Class<out T>> {
        fun loadClass(className: String): Class<out T>? {
            return try {
                val out = appClassLoader.loadClass(className) as Class<T>
                assert(out.classLoader is AppClassLoader)
                if (out.classLoader is AppClassLoader) {
                    println("YES IT IS")
                } else {
                    println("NO IT ISNT")
                }
                println("OUT: ${out.classLoader}")
                out
            } catch (e: ClassCastException) {
                logger.warn("As $className is annotated with ${annotation.qualifiedName} it must be a sub-type of ${type.java.name}")
                null
            } catch (e: Exception) {
                logger.warn("Unable to load class $className", e)
                null
            }
        }

        return getNamesOfClassesWithAnnotation(annotation.java)
                .mapNotNull { loadClass(it) }
                .filterNot { Modifier.isAbstract(it.modifiers) }
    }
}