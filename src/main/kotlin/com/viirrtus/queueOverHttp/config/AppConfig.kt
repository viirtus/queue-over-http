package com.viirrtus.queueOverHttp.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component

@Component
@ConfigurationProperties(prefix = "app")
class AppConfig {
    var brokers: List<BrokerConfig> = listOf()
    var persistence: Map<String, Any> = mutableMapOf()

    companion object {
        const val version = "0.1.0"
    }
}