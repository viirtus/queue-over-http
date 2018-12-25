package com.viirrtus.queueOverHttp.service

import com.viirrtus.queueOverHttp.queue.ConsumerQueueGroup
import com.viirrtus.queueOverHttp.queue.PartitionQueue


interface QueueDrainerInterface {
    /**
     * Handle new consumer in form of group.
     * Drainer must observe group state for
     * registering new queues and unregistering revoked.
     */
    fun add(consumerQueueGroup: ConsumerQueueGroup)

    /**
     * Remove group.
     * All queues from group must be unregistered and removed.
     */
    fun remove(consumerQueueGroup: ConsumerQueueGroup)


    /**
     * Indicate about inner queue state changes.
     * Applicable for notify about queue unlock or new data
     * become available.
     */
    fun onAction(queue: PartitionQueue)
}