package net.corda.nodeapi.internal.serialization.amqp

import java.util.*

/**
 * Helps to keep track where in the object graph current serialization operation is being performed.
 * This applies to either writing or reading.
 *
 * The content will be used for debug purposes to understand where the break was if serialization unexpectedly fails
 */
open class SerializationGraphTracking {

    private val serializationStack = LinkedList<String>()

    fun <T> track(frame: String, block: () -> T): T {
        try {
            serializationStack.add(frame)
            return block()
        } finally {
            serializationStack.removeLast()
        }
    }

    fun prettyPrint(frameFormatFn: StringBuilder.(Int, String) -> Unit = this::multiLineFormatFrame): String {
        val buffer = StringBuilder()
        for((index, value) in serializationStack.withIndex()) {
            buffer.frameFormatFn(index, value)
        }
        return buffer.toString()
    }

    private fun multiLineFormatFrame(buffer: StringBuilder, index: Int, value: String) {
        buffer.append("\t".repeat(index)).append(value).append("\n")
    }

    private fun singleLineFormatFrame(builder: StringBuilder, index: Int, value: String) {
        if(index != 0) {
            builder.append("->")
        }
        builder.append(value)
    }

    override fun toString(): String {
        return prettyPrint(this::singleLineFormatFrame)
    }
}