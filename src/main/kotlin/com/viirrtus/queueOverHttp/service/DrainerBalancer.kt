package com.viirrtus.queueOverHttp.service

import com.viirrtus.queueOverHttp.queue.ConsumerQueueGroup
import com.viirrtus.queueOverHttp.queue.PartitionQueue

/**
 * Balance loading between multiple drainer instances
 * using some strategy or KPI.
 */
interface DrainerBalancer : QueueDrainerInterface {
    override fun onAction(queue: PartitionQueue) {}
}