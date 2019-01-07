package com.viirrtus.queueOverHttp.dto

import com.viirrtus.queueOverHttp.queue.AssociatedMessage
import com.viirrtus.queueOverHttp.service.task.DispatchTask
import com.viirrtus.queueOverHttp.service.task.HttpTaskFactory
import com.viirrtus.queueOverHttp.service.task.TaskExecutorService
import org.springframework.http.HttpMethod
import javax.validation.constraints.NotBlank
import javax.validation.constraints.Pattern

/**
 * General subscription method over http(s).
 */
open class HttpSubscriptionMethod(
        //super properties
        delayOnErrorMs: Long = 0,
        retryBeforeCommit: Long = 0,

        /**
         * URI where subscriber will consume messages
         */
        @field:Pattern(regexp = "http(s)?://.+")
        val uri: String = "",

        /**
         * HTTP method for dispatching.
         * If method support body part, message
         * will be sent in there.
         * If no, message goes to [queryParamKey] as query param.
         */
        val method: HttpMethod = HttpMethod.POST,

        /**
         * For HTTP methods that not support body part.
         * Specify key in which message will be sent.
         */
        @field:NotBlank
        val queryParamKey: String = "message",

        /**
         * Charset for body part or for converting message in query param.
         */
        @field:NotBlank
        val charset: String = "UTF-8",

        /**
         * Some more headers that needs to be included in request.
         * This is a good place to hold any kind of authorization
         */
        val additionalHeaders: Map<String, String> = mapOf<String, String>()
) : SubscriptionMethod(delayOnErrorMs, retryBeforeCommit) {

    override fun createTask(message: AssociatedMessage, executorService: TaskExecutorService): DispatchTask<*> {
        return factory.createTask(message, executorService)
    }

    companion object {
        val factory = HttpTaskFactory()
    }
}