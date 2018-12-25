package com.viirrtus.queueOverHttp.dto

import com.viirrtus.queueOverHttp.queue.AssociatedMessage
import com.viirrtus.queueOverHttp.service.task.DispatchTask
import com.viirrtus.queueOverHttp.service.task.HttpTaskFactory
import com.viirrtus.queueOverHttp.service.task.TaskExecutorService
import org.springframework.http.HttpMethod

/**
 * General subscription method over http(s).
 */
class HttpSubscriptionMethod : SubscriptionMethod() {
    /**
     * URI where subscriber will consume messages
     */
    val uri: String = ""

    /**
     * HTTP method for dispatching.
     * If method support body part, message
     * will be sent in there.
     * If no, message goes to [queryParamKey] as query param.
     */
    val method: HttpMethod = HttpMethod.POST

    /**
     * For HTTP methods that not support body part.
     * Specify key in which message will be sent.
     */
    val queryParamKey: String = "message"

    /**
     * Charset for body part or for converting message in query param.
     */
    val charset: String = "UTF-8"

    /**
     * Some more headers that needs to be included in request.
     * This is a good place to hold any kind of authorization
     */
    val additionalHeaders = mapOf<String, String>()

    override fun createTask(message: AssociatedMessage, executorService: TaskExecutorService): DispatchTask<*> {
        return factory.createTask(message, executorService)
    }

    companion object {
        val factory = HttpTaskFactory()
    }
}