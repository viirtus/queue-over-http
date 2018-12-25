package com.viirrtus.queueOverHttp.util

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.AbstractQueuedSynchronizer

/**
 * This latch is similar to [CountDownLatch] but has internal
 * state and can be reset to initial state.
 *
 * All state changes for less (as opposed to original latch)
 * or equal to zero will
 * send signal to parked at this latch threads.
 */
class StatefulLatch(val initialState: Int) {

    private class Sync internal constructor(val initialCount: Int) : AbstractQueuedSynchronizer() {

        internal val count: Int
            get() = state

        init {
            state = initialCount
        }

        override fun tryAcquireShared(acquires: Int): Int = if (state <= 0) 1 else -1

        override fun tryReleaseShared(releases: Int): Boolean {
            // Decrement current state
            // signal when less than or equal 0
            while (true) {
                val c = state
                //no zero state check!
                val nextc = c - 1
                if (compareAndSetState(c, nextc))
                    return nextc <= 0
            }
        }

        fun tryReset(fromState: Int): Boolean = compareAndSetState(fromState, initialCount)
    }

    private val sync: Sync = Sync(initialState)

    /**
     * Latch current state
     */
    val currentState: Int
        get() = sync.count

    /**
     * Decrement current state
     */
    fun countDown() = sync.releaseShared(1)

    /**
     * Try reset latch to initial state from given [fromState]
     * only if current state are equal to [fromState]
     */
    fun tryReset(fromState: Int): Boolean = sync.tryReset(fromState)

    /**
     * Wait for zero or less state.
     * If state zero or less, it release thread immediately.
     */
    fun await() = sync.acquireSharedInterruptibly(1)

    /**
     * Same as [await] but with timeout.
     */
    fun await(timeout: Long, unit: TimeUnit): Boolean = sync.tryAcquireSharedNanos(1, unit.toNanos(timeout))

}