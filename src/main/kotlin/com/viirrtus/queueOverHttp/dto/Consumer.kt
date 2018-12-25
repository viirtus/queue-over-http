package com.viirrtus.queueOverHttp.dto

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
        val id: String,

        /**
         * Consumer group, i.e. $ServiceName.
         * If there is a numbers consumers with the same group,
         * only one consumer will receive message from topic partition.
         */
        val group: Group,

        /**
         * Method in which consumer must be notified
         * about each new message in subscribed topics.
         */
        val subscriptionMethod: SubscriptionMethod,

        /**
         * Broker name referred to [com.viirrtus.queueOverHttp.config.BrokerConfig.name]
         */
        val broker: String,

        /**
         * List of topics for subscription
         */
        val topics: List<Topic>
) {


    fun same(other: Consumer): Boolean {
        return other.id == id && other.group.id == group.id
    }

    fun toTinyString(): String {
        return "${id}_${group.id}"
    }
}