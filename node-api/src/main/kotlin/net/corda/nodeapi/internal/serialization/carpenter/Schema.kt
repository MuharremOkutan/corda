package net.corda.nodeapi.internal.serialization.carpenter

import kotlin.collections.LinkedHashMap
import org.objectweb.asm.ClassWriter
import org.objectweb.asm.Opcodes.*

/**
 * A Schema is the representation of an object the Carpenter can contsruct
 *
 * Known Sub Classes
 *   - [ClassSchema]
 *   - [InterfaceSchema]
 *   - [EnumSchema]
 */
abstract class Schema(
        val name: String,
        var fields: Map<String, Field>,
        val superclass: Schema? = null,
        val interfaces: List<Class<*>> = emptyList(),
        updater : (String, Field) -> Unit)
{
    private fun Map<String, Field>.descriptors() = LinkedHashMap(this.mapValues { it.value.descriptor })

    init {
        fields.forEach { updater (it.key, it.value) }

        // Fix the order up front if the user didn't, inject the name into the field as it's
        // neater when iterating
        fields = LinkedHashMap(fields)
    }

    fun fieldsIncludingSuperclasses(): Map<String, Field> =
            (superclass?.fieldsIncludingSuperclasses() ?: emptyMap()) + LinkedHashMap(fields)

    fun descriptorsIncludingSuperclasses(): Map<String, String?> =
            (superclass?.descriptorsIncludingSuperclasses() ?: emptyMap()) + fields.descriptors()

    abstract fun generateFields(cw: ClassWriter)

    val jvmName: String
        get() = name.replace(".", "/")

    val asArray: String
        get() = "[L$jvmName;"
}

/**
 * Represents a concrete object
 */
class ClassSchema(
        name: String,
        fields: Map<String, Field>,
        superclass: Schema? = null,
        interfaces: List<Class<*>> = emptyList()
) : Schema(name, fields, superclass, interfaces, { name, field -> field.name = name }) {
    override fun generateFields(cw: ClassWriter) {
        cw.apply { fields.forEach { it.value.generateField(this) }  }
    }
}

/**
 * Represents an interface. Carpented interfaces can be used within [ClassSchema]s
 * if that class should be implementing that interface
 */
class InterfaceSchema(
        name: String,
        fields: Map<String, Field>,
        superclass: Schema? = null,
        interfaces: List<Class<*>> = emptyList()
) : Schema(name, fields, superclass, interfaces, { name, field -> field.name = name }) {
    override fun generateFields(cw: ClassWriter) {
        cw.apply { fields.forEach { it.value.generateField(this) }  }
    }
}

/**
 * Represents an enumerated type
 */
class EnumSchema(
        name: String,
        fields: Map<String, Field>
) : Schema(name, fields, null, emptyList(), { fieldName, field ->
        (field as EnumField).name = fieldName
        field.descriptor = "L${name.replace(".", "/")};"
}) {
    override fun generateFields(cw: ClassWriter) {
        with(cw) {
            fields.forEach { it.value.generateField(this) }

            visitField(ACC_PRIVATE + ACC_FINAL + ACC_STATIC + ACC_SYNTHETIC,
                    "\$VALUES", asArray, null, null)
        }
    }
}

/**
 * Factory object used by the serialiser when build [Schema]s based
 * on an AMQP schema
 */
object CarpenterSchemaFactory {
    fun newInstance(
            name: String,
            fields: Map<String, Field>,
            superclass: Schema? = null,
            interfaces: List<Class<*>> = emptyList(),
            isInterface: Boolean = false
    ) : Schema =
            if (isInterface) InterfaceSchema (name, fields, superclass, interfaces)
            else ClassSchema (name, fields, superclass, interfaces)
}

