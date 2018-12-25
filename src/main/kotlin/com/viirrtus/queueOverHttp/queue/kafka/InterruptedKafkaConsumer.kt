package com.viirrtus.queueOverHttp.queue.kafka

import org.apache.kafka.clients.NetworkClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.internals.ConsumerInterceptors
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient
import org.apache.kafka.clients.consumer.internals.Fetcher
import org.apache.kafka.clients.consumer.internals.SubscriptionState
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.network.Selectable
import org.apache.kafka.common.utils.Time
import java.lang.invoke.MethodHandle
import java.lang.invoke.MethodHandles
import java.lang.invoke.MethodType
import java.time.Duration

class InterruptedKafkaConsumer<K, V>(configs: MutableMap<String, Any>?) : KafkaConsumer<K, V>(configs) {

    private val acquireAndEnsureOpen: MethodHandle
    private val updateAssignmentMetadataIfNeeded: MethodHandle
    private val remainingTimeAtLeastZero: MethodHandle
    private val pollForFetches: MethodHandle
    private val release: MethodHandle

    private val subscriptions: SubscriptionState
    private val client: ConsumerNetworkClient
    private val clientSelector: Selectable
    private val time: Time
    private val fetcher: Fetcher<K, V>
    private val interceptors: ConsumerInterceptors<K, V>

    /**
     * All this file about this small flag
     */
    @Volatile
    private var pollInterrupted = false

    init {
        val lookup = MethodHandles.privateLookupIn(KafkaConsumer::class.java, MethodHandles.lookup())

        val acquireAndEnsureOpenMethodType = MethodType.methodType(Void.TYPE)
        acquireAndEnsureOpen = lookup.findVirtual(KafkaConsumer::class.java, "acquireAndEnsureOpen", acquireAndEnsureOpenMethodType)

        val updateAssignmentMetadataIfNeededMethodType = MethodType.methodType(Boolean::class.javaPrimitiveType, Long::class.javaPrimitiveType)
        updateAssignmentMetadataIfNeeded = lookup.findVirtual(KafkaConsumer::class.java, "updateAssignmentMetadataIfNeeded", updateAssignmentMetadataIfNeededMethodType)

        val remainingTimeAtLeastZeroMethodType = MethodType.methodType(Long::class.javaPrimitiveType, Long::class.javaPrimitiveType, Long::class.javaPrimitiveType)
        remainingTimeAtLeastZero = lookup.findVirtual(KafkaConsumer::class.java, "remainingTimeAtLeastZero", remainingTimeAtLeastZeroMethodType)

        val pollForFetchesMethodType = MethodType.methodType(java.util.Map::class.java, Long::class.javaPrimitiveType)
        pollForFetches = lookup.findVirtual(KafkaConsumer::class.java, "pollForFetches", pollForFetchesMethodType)

        val releaseMethodType = MethodType.methodType(Void.TYPE)
        release = lookup.findVirtual(KafkaConsumer::class.java, "release", releaseMethodType)

        val subscriptionsField = KafkaConsumer::class.java.getDeclaredField("subscriptions")
        subscriptionsField.isAccessible = true
        subscriptions = subscriptionsField.get(this) as SubscriptionState

        val clientField = KafkaConsumer::class.java.getDeclaredField("client")
        clientField.isAccessible = true
        client = clientField.get(this) as ConsumerNetworkClient

        val kafkaClientField = client.javaClass.getDeclaredField("client")
        kafkaClientField.isAccessible = true
        val kafkaClient = kafkaClientField.get(client) as NetworkClient

        val selectorField = kafkaClient.javaClass.getDeclaredField("selector")
        selectorField.isAccessible = true

        clientSelector = selectorField.get(kafkaClient) as Selectable

        val timeField = KafkaConsumer::class.java.getDeclaredField("time")
        timeField.isAccessible = true
        time = timeField.get(this) as Time

        val fetcherField = KafkaConsumer::class.java.getDeclaredField("fetcher")
        fetcherField.isAccessible = true
        fetcher = fetcherField.get(this) as Fetcher<K, V>

        val interceptorsField = KafkaConsumer::class.java.getDeclaredField("interceptors")
        interceptorsField.isAccessible = true
        interceptors = interceptorsField.get(this) as ConsumerInterceptors<K, V>


    }

    override fun poll(timeout: Duration): ConsumerRecords<K, V> {
        return poll(timeout.toMillis(), true)
    }

    private fun poll(timeoutMs: Long, includeMetadataInTimeout: Boolean): ConsumerRecords<K, V> {
        acquireAndEnsureOpen.invokeWithArguments(this)

        try {
            if (timeoutMs < 0) throw IllegalArgumentException("Timeout must not be negative")

            if (subscriptions.hasNoSubscriptionOrUserAssignment()) {
                throw IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions")
            }

            // poll for new data until the timeout expires
            var elapsedTime = 0L
            do {

                client.maybeTriggerWakeup()

                val metadataEnd: Long
                if (includeMetadataInTimeout) {
                    val metadataStart = time.milliseconds()
                    if (!(updateAssignmentMetadataIfNeeded.invokeWithArguments(this, remainingTimeAtLeastZero.invokeWithArguments(this, timeoutMs, elapsedTime)) as Boolean)) {
                        return ConsumerRecords.empty()
                    }
                    metadataEnd = time.milliseconds()
                    elapsedTime += metadataEnd - metadataStart
                } else {
                    while (!(updateAssignmentMetadataIfNeeded.invokeWithArguments(this, java.lang.Long.MAX_VALUE) as Boolean)) {

                    }
                    metadataEnd = time.milliseconds()
                }

                val records = pollForFetches.invokeWithArguments(this, remainingTimeAtLeastZero.invokeWithArguments(this, timeoutMs, elapsedTime)) as Map<TopicPartition, List<ConsumerRecord<K, V>>>

                if (!records.isEmpty()) {
                    // before returning the fetched records, we can send off the next round of fetches
                    // and avoid block waiting for their responses to enable pipelining while the user
                    // is handling the fetched records.
                    //
                    // NOTE: since the consumed position has already been updated, we must not allow
                    // wakeups or any other errors to be triggered prior to returning the fetched records.
                    if (fetcher.sendFetches() > 0 || client.hasPendingRequests()) {
                        client.pollNoWakeup()
                    }

                    return interceptors.onConsume(ConsumerRecords(records))
                }
                val fetchEnd = time.milliseconds()
                elapsedTime += fetchEnd - metadataEnd

            } while (elapsedTime < timeoutMs && !pollInterrupted)

            pollInterrupted = false
            return ConsumerRecords.empty()
        } finally {
            release.invokeWithArguments(this)
        }
    }

    /**
     * Key feature.
     * Interrupt poll loop without raising wakeup exception.
     */
    fun interruptPoll() {
        clientSelector.wakeup()
        pollInterrupted = true
    }
}