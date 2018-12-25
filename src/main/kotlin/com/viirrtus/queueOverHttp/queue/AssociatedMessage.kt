package com.viirrtus.queueOverHttp.queue

import com.viirrtus.queueOverHttp.dto.Consumer
import com.viirrtus.queueOverHttp.service.task.DispatchTask
import com.viirrtus.queueOverHttp.service.task.TaskExecutorService

/**
 * Keep together all data for dispatching
 */
data class AssociatedMessage(
        /**
         * To be sent to consumer
         */
        val message: Message,

        /**
         * Consumer to be notified
         */
        val consumer: Consumer,

        /**
         * Queue from which message came
         */
        val queue: PartitionQueue
) {
    /**
     * Mark message as completed
     */
    fun commit() {
        queue.commit(message.number)
    }

    /**
     * Convert message to dispatch task
     */
    fun toTask(executorService: TaskExecutorService): DispatchTask<*> {
        return consumer.subscriptionMethod.createTask(this, executorService)
    }

    override fun toString(): String {
        return "${this::class.java.simpleName}[${queue.associatedPartition}, $message]"
    }
}