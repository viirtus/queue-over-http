package com.viirrtus.queueOverHttp.queue

/**
 * Drained from native queue message.
 */
data class Message(
        /**
         * Message content
         */
        val body: String,

        /**
         * Message number from native queue numbering implementation
         */
        val number: Long,

        /**
         * Applicable only for Kafka.
         * Message partition key if specified.
         */
        val key: String?
) {
    override fun toString(): String {
        return "${this::class.java.simpleName}[$body, $number]"
    }
}
