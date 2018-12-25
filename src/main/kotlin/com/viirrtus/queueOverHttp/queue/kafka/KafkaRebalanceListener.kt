package com.viirrtus.queueOverHttp.queue.kafka

import com.viirrtus.queueOverHttp.queue.ConsumerQueueGroup
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

/**
 * Listen rebalance event of native Kafka client for given [queueGroup]
 */
class KafkaRebalanceListener(private val queueGroup: ConsumerQueueGroup) : ConsumerRebalanceListener {

    /**
     * Add assigned partition to consumer queue group
     */
    override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>) {
        for (partition in partitions) {
            val ourTopicPartition = partition.toLocalTopicPartition()
            val queue = queueGroup.createPartitionQueue(ourTopicPartition)
            queueGroup.add(queue)
        }

        logger.info("Assigned partitions to consumer (${queueGroup.consumer}): \n"
                + partitions.joinToString()
        )
    }

    /**
     * Remove assigned partition from consumer queue group
     */
    override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>) {
        for (partition in partitions) {
            val queue = queueGroup.findAssociated(partition.toLocalTopicPartition())
            if (queue != null) {
                queueGroup.remove(queue)
            }
        }
        logger.info("Revoked partitions from consumer (${queueGroup.consumer}): \n"
                + partitions.joinToString()
        )
    }

    companion object {
        private val logger = LoggerFactory.getLogger(this::class.java)
    }
}