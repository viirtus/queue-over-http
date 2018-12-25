package com.viirrtus.queueOverHttp.service

import com.viirrtus.queueOverHttp.queue.ConsumerQueueGroup

/**
 * Balancer with round-robin strategy.
 */
class RoundRobinDrainerBalancer(
        private val locator: ComponentLocator
) : DrainerBalancer {
    //TODO externalize
    private val size: Int = 1

    private val brokers = (0 until size).map { locator.locate(QueueDrainer::class) }

    private val brokersIterator = RRIterator(brokers)

    init {
        brokers.forEach { Thread(it).start() }
    }

    override fun add(consumerQueueGroup: ConsumerQueueGroup) {
        val broker = brokersIterator.next()
        broker.add(consumerQueueGroup)
    }

    override fun remove(consumerQueueGroup: ConsumerQueueGroup) {
        brokers.forEach { it.remove(consumerQueueGroup) }
    }

    private class RRIterator<T>(private val list: List<T>) : Iterator<T> {
        private var position = 0

        override fun hasNext(): Boolean {
            return true
        }

        override fun next(): T {
            val el = list[position++]
            position %= list.size

            return el
        }

    }
}