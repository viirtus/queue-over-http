package com.viirrtus.queueOverHttp.queue

enum class CommitMode {
    /**
     * Regular commit based on load factor
     */
    NORMAL,

    /**
     * Auto commit by time interval
     */
    FORCE
}