package com.viirrtus.queueOverHttp.config

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.viirrtus.queueOverHttp.service.ComponentLocator
import com.viirrtus.queueOverHttp.service.DrainerBalancer
import com.viirrtus.queueOverHttp.service.RoundRobinDrainerBalancer
import com.viirrtus.queueOverHttp.service.persistence.FilePersistenceAdapter
import com.viirrtus.queueOverHttp.service.persistence.NoopPersistenceAdapter
import com.viirrtus.queueOverHttp.service.persistence.PersistenceAdapterInterface
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration


@Configuration
open class SpringContextConfig {
    @Bean
    open fun objectMapper(): ObjectMapper {
        val mapper = ObjectMapper()
        mapper.registerModule(JavaTimeModule())
        mapper.registerModule(KotlinModule())
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)

        return mapper
    }

    @Bean
    open fun brokerBalancer(componentLocator: ComponentLocator): DrainerBalancer {
        return RoundRobinDrainerBalancer(componentLocator)
    }

    @Bean
    open fun persistenceAdapter(componentLocator: ComponentLocator, appConfig: AppConfig): PersistenceAdapterInterface {
        if (appConfig.persistence.containsKey("file")) {
            return componentLocator.locate(FilePersistenceAdapter::class)
        }

        return NoopPersistenceAdapter()
    }

}