package com.viirrtus.queueOverHttp.queue

import com.viirrtus.queueOverHttp.config.BrokerConfig
import com.viirrtus.queueOverHttp.dto.Consumer
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Adapter over native queue client.
 *
 * [T] - native consumer client
 */
abstract class NativeQueueAdapter<T>(
        /**
         * Initial app config for this instance
         */
        protected val brokerConfig: BrokerConfig
) : QueueInterface {

    /**
     * List of all registered consumers in bind with
     * native client consumer.
     */
    protected val consumers = CopyOnWriteArrayList<NativeConsumerWrapper<T>>()

    /**
     * List all registered consumers.
     */
    fun listConsumers(): List<Consumer> {
        return consumers.asSequence().map { it.group.consumer }.toList()
    }

    /**
     * Find consumer group
     */
    fun findGroup(consumer: Consumer): ConsumerQueueGroup? {
        return consumers.find { it.group.consumer.same(consumer) }?.group
    }

    /**
     * Check if given [consumer] are already registered in
     * this broker.
     */
    protected fun isAlreadyRegistered(consumer: Consumer): Boolean {
        return consumers.any { it.group.consumer.same(consumer) }
    }

    open class NativeConsumerWrapper<T>(
            /**
             * Native client
             */
            val consumer: T,

            /**
             * Associated queue group
             */
            val group: ConsumerQueueGroup
    )
}