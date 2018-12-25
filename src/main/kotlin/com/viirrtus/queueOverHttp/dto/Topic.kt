package com.viirrtus.queueOverHttp.dto

import com.viirrtus.queueOverHttp.queue.LocalTopicPartition
import java.util.regex.Pattern

/**
 * Native queue topic name for which consumer subscribe.
 */
data class Topic(
        val name: String,

        /**
         * Now, if this list not empty, Kafka native adapter disable
         * automatic partition management.
         */
        val partitions: List<Partition> = emptyList(),

        /**
         * Consume strategy configuration.
         */
        val config: Config,

        /**
         * If true, name will be interpreted as topic
         * name regexp pattern.
         */
        val pattern: Boolean = false
) {
    fun matched(localTopicPartition: LocalTopicPartition): Boolean {
        return if (pattern) {
            val pattern = Pattern.compile(name)
            pattern.matcher(localTopicPartition.topic).matches()
        } else {
            name == localTopicPartition.topic
        }
    }
}