package com.viirrtus.queueOverHttp.dto

import javax.validation.Valid
import javax.validation.constraints.NotBlank
import javax.validation.constraints.NotEmpty

/**
 * Container for any kind of configuration to identify
 * subscription.
 *
 * Usually, consumer is a microservice or independent controller of your infrastructure.
 *
 * Consumer can listen any number of topics.
 */
data class Consumer(
        /**
         * Consumer ID. Must be uniq over all consumers.
         * In general, can be `$ServiceName-$ServiceReplica`.
         */
        @field:NotBlank
        val id: String,

        /**
         * Consumer group, i.e. $ServiceName.
         * If there is a numbers consumers with the same group,
         * only one consumer will receive message from topic partition.
         */
        @field:Valid
        val group: Group,

        /**
         * Method in which consumer must be notified
         * about each new message in subscribed topics.
         */
        @field:Valid
        val subscriptionMethod: SubscriptionMethod,

        /**
         * Broker name referred to [com.viirrtus.queueOverHttp.config.BrokerConfig.name]
         */
        @field:NotBlank
        val broker: String,

        /**
         * List of topics for subscription
         */
        @field:NotEmpty
        @field:Valid
        val topics: List<Topic>
) {


    fun same(other: Consumer): Boolean {
        return other.id == id && other.group.id == group.id
    }

    fun toTinyString(): String {
        return "${id}_${group.id}"
    }

    override fun toString(): String {
        return toTinyString()
    }
}