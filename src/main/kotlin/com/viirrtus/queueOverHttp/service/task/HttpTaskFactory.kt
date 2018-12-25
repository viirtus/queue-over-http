package com.viirrtus.queueOverHttp.service.task

import com.viirrtus.queueOverHttp.queue.AssociatedMessage

class HttpTaskFactory : TaskFactory() {
    /**
     * Create tasks with dispatcher inside.
     */
    override fun createTask(message: AssociatedMessage, executorService: TaskExecutorService): DispatchTask<*> {
        return HttpDispatchTask(message, executorService, dispatcher)
    }

    companion object {
        private val dispatcher = HttpMessageDispatcher()
    }
}