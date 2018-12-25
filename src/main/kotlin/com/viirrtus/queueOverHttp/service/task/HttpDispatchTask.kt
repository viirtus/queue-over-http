package com.viirrtus.queueOverHttp.service.task

import com.viirrtus.queueOverHttp.dto.HttpSubscriptionMethod
import com.viirrtus.queueOverHttp.queue.AssociatedMessage
import org.apache.http.HttpResponse
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import java.util.concurrent.CompletableFuture

/**
 * Task for dispatching [message] over HTTP.
 */
class HttpDispatchTask(
        message: AssociatedMessage,
        executorService: TaskExecutorService,
        private val dispatcher: HttpMessageDispatcher
) : DispatchTask<HttpResponse>(message, executorService) {
    private val httpSubscriptionMethod = subscriptionMethod as HttpSubscriptionMethod

    override fun execute(): CompletableFuture<HttpResponse> {
        val uri = httpSubscriptionMethod.uri
        return dispatcher.dispatch(uri, message)
    }

    /**
     * Task successfully executed when we receive a non 4xx or 5xx
     * status code in reply.
     */
    override fun onTaskCompleted(result: HttpResponse) {
        val status = HttpStatus.valueOf(result.statusLine.statusCode)
        if (!status.isError) {
            super.onTaskCompleted(result)
        } else {
            logger.warn("Error while executing task $this over HTTP. Consumer response with $status status (${result.statusLine}).")
            onTaskError()
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(HttpDispatchTask::class.java)
    }
}