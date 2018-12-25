package com.viirrtus.queueOverHttp.queue

import com.viirrtus.queueOverHttp.dto.Consumer
import com.viirrtus.queueOverHttp.dto.Topic

interface QueueInterface {
    /**
     * Subscribe consumer to queue
     */
    fun subscribe(consumer: Consumer): ConsumerQueueGroup

    /**
     * Unsubscribe consumer from queue
     */
    fun unsubscribe(consumer: Consumer)

    /**
     * Push [message] to [topic] queue
     */
    fun produce(message: Message, topic: Topic)

    /**
     * Commit message with [messageNumber] to [queue]
     */
    fun commit(queue: PartitionQueue, messageNumber: Long, mode: CommitMode)
}