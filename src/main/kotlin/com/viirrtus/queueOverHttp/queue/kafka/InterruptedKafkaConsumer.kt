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
        fun findPrivateMethod(name: String, vararg args: Class<*>?): MethodHandle {
            val method = KafkaConsumer::class.java.getDeclaredMethod(name, *args)
            method.isAccessible = true
            return MethodHandles.lookup().unreflect(method)
        }

        fun findPrivateField(name: String): Any {
            val field = KafkaConsumer::class.java.getDeclaredField(name)
            field.isAccessible = true
            return field.get(this)
        }

        acquireAndEnsureOpen = findPrivateMethod("acquireAndEnsureOpen")
        updateAssignmentMetadataIfNeeded = findPrivateMethod("updateAssignmentMetadataIfNeeded", Long::class.javaPrimitiveType)
        remainingTimeAtLeastZero = findPrivateMethod("remainingTimeAtLeastZero", Long::class.javaPrimitiveType, Long::class.javaPrimitiveType)
        pollForFetches = findPrivateMethod("pollForFetches", Long::class.javaPrimitiveType)
        release = findPrivateMethod("release")

        client = findPrivateField("client") as ConsumerNetworkClient
        subscriptions = findPrivateField("subscriptions") as SubscriptionState
        time = findPrivateField("time") as Time
        fetcher = findPrivateField("fetcher") as Fetcher<K, V>
        interceptors = findPrivateField("interceptors") as ConsumerInterceptors<K, V>

        val kafkaClientField = client.javaClass.getDeclaredField("client")
        kafkaClientField.isAccessible = true
        val kafkaClient = kafkaClientField.get(client) as NetworkClient

        val selectorField = kafkaClient.javaClass.getDeclaredField("selector")
        selectorField.isAccessible = true

        clientSelector = selectorField.get(kafkaClient) as Selectable
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