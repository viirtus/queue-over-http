package com.viirrtus.queueOverHttp.config

import com.viirrtus.queueOverHttp.queue.NativeQueueAdapter
import com.viirrtus.queueOverHttp.queue.kafka.KafkaNativeQueueAdapter
import org.springframework.stereotype.Component
import kotlin.reflect.KClass

@Component
class BrokerConfig {
    /**
     * Name of broker which will be used by consumers for subscription
     */
    lateinit var name: String

    /**
     * Broker bind to adapter
     */
    lateinit var origin: Origin

    /**
     * Any KV that supported by broker client implementation
     * used in this project
     */
    lateinit var config: Map<String, String>

    enum class Origin(val clazz: KClass<out NativeQueueAdapter<*>>) {
        KAFKA(KafkaNativeQueueAdapter::class)
    }
}