package com.viirrtus.queueOverHttp.service.task

import com.viirrtus.queueOverHttp.queue.AssociatedMessage

/**
 * Factory for concrete task creation.
 */
abstract class TaskFactory {
    abstract fun createTask(message: AssociatedMessage, executorService: TaskExecutorService): DispatchTask<*>
}