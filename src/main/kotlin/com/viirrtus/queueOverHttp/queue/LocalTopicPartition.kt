package com.viirrtus.queueOverHttp.queue

/**
 * Local implementation of partition of topic.
 */
data class LocalTopicPartition(
        val topic: String,
        val partition: String
) {
    override fun toString(): String {
        return "${this::class.java.simpleName}[$topic, $partition]"
    }
}