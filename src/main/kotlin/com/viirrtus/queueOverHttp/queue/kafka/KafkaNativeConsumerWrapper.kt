package com.viirrtus.queueOverHttp.queue.kafka

import com.viirrtus.queueOverHttp.Application
import com.viirrtus.queueOverHttp.queue.*
import com.viirrtus.queueOverHttp.util.hiResCurrentTimeNanos
import org.apache.kafka.clients.NetworkClient
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.AuthorizationException
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.network.Selectable
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.LinkedBlockingQueue


class KafkaNativeConsumerWrapper(consumer: InterruptedKafkaConsumer<String, String>, group: ConsumerQueueGroup)
    : NativeQueueAdapter.NativeConsumerWrapper<InterruptedKafkaConsumer<String, String>>(consumer, group), Runnable {
    private val commitRequests = LinkedBlockingQueue<CommitRequest>()

    @Volatile
    private var isConsumerUnsubscribed = false


    override fun run() {
        Thread.currentThread().name = "Kafka-${group.consumer.broker}-${group.consumer.group.id}-${group.consumer.id}"

        while (!isConsumerUnsubscribed && !Thread.interrupted()) {

            try {
                processCommitRequests()
                invalidateSubscription()

                // poll will be interrupted in case of new commit or unsubscribe requests
                val records = consumer.poll(Duration.ofMillis(100))

                //optimize next iterators creation for nothing
                if (records.isEmpty) {
                    continue
                }

                val partitionToRecordsMap = records.groupBy {
                    LocalTopicPartition(it.topic(), it.partition().toString())
                }

                for (prEntry in partitionToRecordsMap) {
                    val partition = prEntry.key
                    val partitionRecords = prEntry.value
                    val queue = group.findAssociated(partition) ?: continue

                    if (Application.DEBUG) {
                        logger.debug("Push ${partitionRecords.size} records to $partition.")
                    }

                    queue.push(partitionRecords.map { Message(it.value(), it.offset(), it.key()) })
                }

            } catch (e: Exception) {
                logger.warn("Caught unexpected exception while working with $group. Consumer will be unsubscribed.", e)
                //ref to this is still holding by adapter

                group.clear()
                doUnsubscribe()
            }
        }

        doUnsubscribe()
    }


    /**
     * Realize back pressure for draining queue.
     * If partition queue are full, it will be paused.
     * If paused queue wanna more messages, it will be resumed.
     *
     * Paused queue not receive anymore new items.
     *
     * Also, invalidate revoked queues.
     * Revoked queues will be removed from group.
     */
    private fun invalidateSubscription() {
        for (queue in group) {
            val status = queue.status
            if (status != PartitionQueueStatus.REVOKED) {
                if (status.wannaMore) {
                    if (queue.isPaused) {
                        val topicPartition = queue.associatedPartition.toKafkaTopicPartition()
                        consumer.resume(listOf(topicPartition))
                        queue.resume()
                    }
                } else {
                    if (!queue.isPaused) {
                        val topicPartition = queue.associatedPartition.toKafkaTopicPartition()
                        consumer.pause(listOf(topicPartition))
                        queue.pause()
                    }
                }
            } else {
                group.remove(queue)
            }
        }
    }

    /**
     * Commit offset for specified [queue] for this consumer.
     *
     * Long poll of native consumer will be interrupted for immediately process commit.
     */
    fun commit(queue: PartitionQueue, messageNumber: Long, mode: CommitMode) {
        commitRequests.add(CommitRequest(queue, messageNumber, mode))
        consumer.interruptPoll()
    }

    private fun processCommitRequests() {
        //optimize iterator creation for nothing
        if (commitRequests.isEmpty()) {
            return
        }

        getPreparedCommitWrapper().forEach { consumer, wrapper ->
            var failCounter = 0
            do {
                var shouldRetry = false
                try {
                    wrapper.requests.forEach { it.queue.onBeforeCommit(it.offset, it.mode) }

                    consumer.commitSync(wrapper.topicsData)

                    wrapper.requests.forEach { it.queue.onAfterCommit(it.offset, it.mode) }
                } catch (e: Exception) {
                    when (e) {
                        is CommitFailedException,
                        is AuthenticationException,
                        is AuthorizationException,
                        is IllegalArgumentException,
                        is InterruptException,
                        is KafkaException -> {

                            // this is unrecoverable exception, there is nothing to do
                            // but notify queue
                            logger.error("Cannot commit topics: ${wrapper.topicsData}", e)
                            wrapper.requests.forEach { it.queue.onCommitFailed(it.offset, it.mode, e) }
                        }

                        else -> {

                            //kind of recoverable exception
                            //we must retry after some time
                            shouldRetry = true
                            val sleepTimeMs = Math.min(Math.abs(++failCounter) * 2, 1000)

                            //this is block next commit request (and dispatching as a side effect)
                            //but there is no problem because
                            //next request also will fail
                            Thread.sleep(sleepTimeMs.toLong())
                        }
                    }
                }
            } while (shouldRetry && !Thread.interrupted())
        }

    }

    private fun getPreparedCommitWrapper(): Map<KafkaConsumer<String, String>, CommitActionMetadata> {
        val requests = mutableListOf<CommitRequest>()

        //drain available tasks
        //because there only one place for
        //polling queue, this will never block
        repeat(commitRequests.size) {
            requests.add(commitRequests.poll())
        }

        val consumerToTopics = mutableMapOf<KafkaConsumer<String, String>, CommitActionMetadata>()

        // multiplex requests from the same native consumer into one
        for (request in requests) {
            val commitWrapper = consumerToTopics.computeIfAbsent(consumer) {
                CommitActionMetadata()
            }

            val partitions = request.queue.associatedPartition.toKafkaOffsetMap(request.offset)
            commitWrapper.topicsData.putAll(partitions)

            //keep original request for notify queue
            commitWrapper.requests.add(request)

            if (Application.DEBUG) {
                logger.debug("Commit processed in ${hiResCurrentTimeNanos() - request.creationTime} nanos.")
            }
        }

        return consumerToTopics
    }

    fun unsubscribe() {
        isConsumerUnsubscribed = true

        consumer.interruptPoll()
    }

    /**
     * Unsubscribe native consumer
     */
    private fun doUnsubscribe() {
        try {
            consumer.unsubscribe()
            consumer.close()
        } catch (e: Exception) {
            logger.warn("An error occurred while unsubscribe.", e)
        }
    }

    data class CommitActionMetadata(
            val requests: MutableList<CommitRequest> = mutableListOf(),
            val topicsData: MutableMap<TopicPartition, OffsetAndMetadata> = mutableMapOf()
    )

    data class CommitRequest(
            val queue: PartitionQueue,
            val offset: Long,
            val mode: CommitMode,
            val creationTime: Long = hiResCurrentTimeNanos()
    )

    companion object {
        private val logger = LoggerFactory.getLogger(this::class.java)
    }
}

fun LocalTopicPartition.toKafkaOffsetMap(lastConsumedMessageNumber: Long): MutableMap<TopicPartition, OffsetAndMetadata> {
    //must commit NEXT message number
    val offset = OffsetAndMetadata(lastConsumedMessageNumber + 1)
    return mutableMapOf(toKafkaTopicPartition() to offset)
}

fun com.viirrtus.queueOverHttp.queue.LocalTopicPartition.toKafkaTopicPartition(): TopicPartition {
    return TopicPartition(topic, partition.toInt())
}

fun TopicPartition.toLocalTopicPartition(): com.viirrtus.queueOverHttp.queue.LocalTopicPartition {
    return com.viirrtus.queueOverHttp.queue.LocalTopicPartition(topic(), partition().toString())
}