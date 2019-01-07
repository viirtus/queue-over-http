package com.viirrtus.queueOverHttp.controller

import com.viirrtus.queueOverHttp.config.AppConfig
import com.viirrtus.queueOverHttp.service.task.HttpMessageDispatcher
import org.springframework.context.annotation.Profile
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.ResponseBody
import org.springframework.web.bind.annotation.RestController
import java.util.*
import javax.servlet.http.HttpServletRequest

/**
 * Used only for inplace testing
 */
@RestController
@Profile("test")
class TestController(
        val config: AppConfig
) {

    @PostMapping("/test")
    @ResponseBody
    fun test(@RequestBody(required = false) body: String?, request: HttpServletRequest): ResponseEntity<String> {
        receivedMessages.add(
                TestMessage(
                        body,
                        request.getHeader(HttpMessageDispatcher.CONSUMER_HEADER),
                        request.getHeader(HttpMessageDispatcher.TOPIC_HEADER),
                        request.getHeader(HttpMessageDispatcher.PARTITION_HEADER),
                        request.getHeader(HttpMessageDispatcher.BROKER_HEADER),
                        request.getHeader(HttpMessageDispatcher.KEY_HEADER),
                        request.getHeader(HttpMessageDispatcher.NUMBER_HEADER)
                )
        )
        return ResponseEntity.ok("OK")
    }

    companion object {
        var receivedMessages = Collections.synchronizedList(mutableListOf<TestMessage>())
    }

    data class TestMessage(
            val body: String?,
            val consumerHeader: String,
            val topicHeader: String,
            val partitionHeader: String,
            val brokerHeader: String,
            val keyHeader: String,
            val numberHeader: String
    )
}