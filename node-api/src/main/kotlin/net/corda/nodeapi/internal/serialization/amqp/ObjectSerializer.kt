package net.corda.nodeapi.internal.serialization.amqp

import net.corda.core.utilities.debug
import net.corda.core.utilities.loggerFor
import net.corda.nodeapi.internal.serialization.amqp.SerializerFactory.Companion.nameForType
import org.apache.qpid.proton.amqp.Symbol
import org.apache.qpid.proton.codec.Data
import java.io.NotSerializableException
import java.lang.reflect.Type
import kotlin.reflect.jvm.javaConstructor

/**
 * Responsible for serializing and de-serializing a regular object instance via a series of properties (matched with a constructor).
 */
open class ObjectSerializer(val clazz: Type, factory: SerializerFactory) : AMQPSerializer<Any> {
    override val type: Type get() = clazz
    open val kotlinConstructor = constructorForDeserialization(clazz)
    private val javaConstructor by lazy { kotlinConstructor?.javaConstructor }

    private val logger = loggerFor<ObjectSerializer>()

    open internal val propertySerializers: Collection<PropertySerializer> by lazy {
        propertiesForSerialization(kotlinConstructor, clazz, factory)
    }

    private val typeName = nameForType(clazz)

    override val typeDescriptor: Symbol = Symbol.valueOf("$DESCRIPTOR_DOMAIN:${fingerprintForType(clazz, factory)}")
    private val interfaces = interfacesForSerialization(clazz, factory) // We restrict to only those annotated or whitelisted

    open internal val typeNotation: TypeNotation by lazy { CompositeType(typeName, null, generateProvides(), Descriptor(typeDescriptor), generateFields()) }

    override fun writeClassInfo(output: SerializationOutput) {
        if (output.writeTypeNotations(typeNotation)) {
            for (iface in interfaces) {
                output.requireSerializer(iface)
            }
            for (property in propertySerializers) {
                property.writeClassInfo(output)
            }
        }
    }

    override fun writeObject(obj: Any, data: Data, type: Type, output: SerializationOutput) {
        // Write described
        data.withDescribed(typeNotation.descriptor) {
            // Write list
            withList {
                for (property in propertySerializers) {
                    property.writeProperty(obj, this, output)
                }
            }
        }
    }

    override fun readObject(obj: Any, schema: Schema, input: DeserializationInput): Any = input.track(clazz.typeName) {
        if (obj is List<*>) {
            if (obj.size > propertySerializers.size) throw NotSerializableException("Too many properties in described type $typeName. Path in the graph:\n" + input.prettyPrint())
            val params = obj.zip(propertySerializers).map { it.second.readProperty(it.first, schema, input) }
            construct(params, input)
        } else throw NotSerializableException("Body of described type is unexpected $obj. Path in the graph:\n" + input.prettyPrint())
    }

    private fun generateFields(): List<Field> {
        return propertySerializers.map { Field(it.name, it.type, it.requires, it.default, null, it.mandatory, false) }
    }

    private fun generateProvides(): List<String> = interfaces.map { nameForType(it) }

    fun construct(properties: List<Any?>, graphTracking: SerializationGraphTracking): Any {

        logger.debug { "Calling constructor: '$javaConstructor' with properties '$properties'" }

        return javaConstructor?.newInstance(*properties.toTypedArray()) ?:
                throw NotSerializableException("Attempt to deserialize an interface: $clazz." +
                        " Serialized form is invalid. Path in the graph:\n" + graphTracking.prettyPrint())
    }
}