package com.viirrtus.queueOverHttp.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component

@Component
@ConfigurationProperties(prefix = "app.persistence.file")
class FilePersistenceConfig {
    var storageDirectory: String = ""
}