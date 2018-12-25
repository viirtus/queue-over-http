package com.viirrtus.queueOverHttp.service.persistence

import com.viirrtus.queueOverHttp.dto.Consumer

/**
 * Persistence is useful for keep subscriptions between
 * service starts, or, for recovery in case of error.
 */
interface PersistenceAdapterInterface {
    /**
     * Store consumer in persistence for future.
     */
    fun store(consumer: Consumer)

    /**
     * Remove consumer from storage.
     * Removed consumer must not be restored anymore.
     */
    fun remove(consumer: Consumer)

    /**
     * Fetch all stored before consumers.
     */
    fun restore(): List<Consumer>
}