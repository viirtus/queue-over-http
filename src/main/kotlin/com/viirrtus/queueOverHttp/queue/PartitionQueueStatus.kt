package com.viirrtus.queueOverHttp.queue

/**
 * [wannaMore] - indicate that queue can consume more messages
 */
enum class PartitionQueueStatus(val wannaMore: Boolean) {
    EMPTY(true),
    READY(true),
    FULL(false),
    REVOKED(false),
    READY_TO_DIE(false),
    LOCKED(false),
}