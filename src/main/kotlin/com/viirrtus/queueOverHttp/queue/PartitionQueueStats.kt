package com.viirrtus.queueOverHttp.queue

data class PartitionQueueStats(
        val lastNormalCommitMessageNumber: Long,
        val lastCommitMessageNumber: Long,
        val lastForceCommitRequestMessageNumber: Long,
        val lastConsumedMessageNumber: Long,
        val isPaused: Boolean,
        val isRevoked: Boolean
) {
    override fun toString(): String {
        return "[lncmn=$lastNormalCommitMessageNumber," +
                " lcmn=$lastCommitMessageNumber," +
                " lfcrmn=$lastForceCommitRequestMessageNumber," +
                " lcmn=$lastConsumedMessageNumber," +
                " paused=$isPaused," +
                " revoked=$isRevoked," +
                " ]"
    }
}