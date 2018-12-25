package com.viirrtus.queueOverHttp.dto

/**
 * Consume strategy.
 */
data class Config(
        /**
         * Max parallel dispatcher requests that can be send to
         * consumer from this topic.
         *
         * Also, mean that this is a max messages that will be
         * send to consumer twice after service fail recovery from this queue.
         *
         * Each [concurrencyFactor] messages must be commited to native queue.
         */
        val concurrencyFactor: Int = 1,

        /**
         * In additional to commit after each [concurrencyFactor] messages
         * dispatched, this option allows to specify time tick in which
         * messages will be commited to native queue if they are already where dispatched.
         */
        val autoCommitPeriodMs: Long = 0
)