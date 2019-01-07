package com.viirrtus.queueOverHttp.dto

import com.viirrtus.queueOverHttp.queue.LocalTopicPartition
import java.util.regex.Pattern
import javax.validation.Valid
import javax.validation.constraints.NotBlank

/**
 * Native queue topic name for which consumer subscribe.
 */
data class Topic(
        @field:NotBlank
        val name: String,

        /**
         * Now, if this list not empty, Kafka native adapter disable
         * automatic partition management.
         */
        @field:Valid
        val partitions: List<Partition> = emptyList(),

        /**
         * Consume strategy configuration.
         */
        @field:Valid
        val config: Config,

        /**
         * If true, name will be interpreted as topic
         * name regexp pattern.
         */
        val pattern: Boolean = false
) {

    /**
     * Check if [localTopicPartition] are matched by topic name with current
     */
    fun matched(localTopicPartition: LocalTopicPartition): Boolean {
        return if (pattern) {
            val pattern = Pattern.compile(name)
            pattern.matcher(localTopicPartition.topic).matches()
        } else {
            name == localTopicPartition.topic
        }
    }
}