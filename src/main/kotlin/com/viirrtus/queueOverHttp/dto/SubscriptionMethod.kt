package com.viirrtus.queueOverHttp.dto

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.viirrtus.queueOverHttp.queue.AssociatedMessage
import com.viirrtus.queueOverHttp.service.task.DispatchTask
import com.viirrtus.queueOverHttp.service.task.TaskExecutorService
import javax.validation.constraints.PositiveOrZero


/**
 * Method in which consumer must be notified about new messages.
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type"
)
@JsonSubTypes(
        JsonSubTypes.Type(value = HttpSubscriptionMethod::class, name = "http")
)
abstract class SubscriptionMethod(
        /**
         * If dispatcher fails to sent message,
         * this option allows to specify delay in
         * milliseconds before next try.
         */
        @field:PositiveOrZero
        val delayOnErrorMs: Long = 0,

        /**
         * Specify max retry attempts count before message will be commited
         * (i.e. mark as completed).
         *
         * If 0, message keep rolling infinity, unless dispatching
         * becomes successfully.
         */
        @field:PositiveOrZero
        val retryBeforeCommit: Long = 0
) {
    /**
     * Create dispatching task for this method.
     */
    abstract fun createTask(message: AssociatedMessage, executorService: TaskExecutorService): DispatchTask<*>
}