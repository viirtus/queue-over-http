package com.viirrtus.queueOverHttp.service.persistence

import com.viirrtus.queueOverHttp.dto.Consumer
import org.springframework.stereotype.Component

/**
 * Nothing to do!
 */
@Component
class NoopPersistenceAdapter : PersistenceAdapterInterface {
    override fun store(consumer: Consumer) {}

    override fun remove(consumer: Consumer) {}

    override fun restore(): List<Consumer> {
        return emptyList()
    }
}