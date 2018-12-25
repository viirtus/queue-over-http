package com.viirrtus.queueOverHttp.controller

import com.viirrtus.queueOverHttp.dto.Consumer
import com.viirrtus.queueOverHttp.service.BrokerService
import org.springframework.web.bind.annotation.*
import java.util.concurrent.TimeUnit

@RestController
@RequestMapping("/broker")
class BrokerController(
        private val brokerService: BrokerService
) {

    /**
     * Subscribe consumer to specified topics of broker.
     */
    @PostMapping("subscription")
    @ResponseBody
    fun subscribe(@RequestBody consumer: Consumer): Response<String> {
        brokerService.subscribe(consumer)

        return Response("OK")
    }

    /**
     * List all registered consumers over all brokers
     */
    @GetMapping("subscription")
    @ResponseBody
    fun list(): Response<List<Consumer>> {
        return Response(brokerService.list())
    }

    /**
     * Unsubscribe (i.e. unregister) consumer.
     */
    @PostMapping("unsubscribe")
    @ResponseBody
    fun unsubscribe(@RequestBody consumer: Consumer): Response<String> {
        val unsubscribeWaitTimeoutMs = 20_000L
        brokerService.unsubscribe(consumer, unsubscribeWaitTimeoutMs, TimeUnit.MILLISECONDS)

        return Response("OK")
    }
}