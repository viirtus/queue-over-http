package com.viirrtus.queueOverHttp.queue.kafka

import com.viirrtus.queueOverHttp.config.BrokerConfig
import com.viirrtus.queueOverHttp.dto.Consumer
import com.viirrtus.queueOverHttp.dto.Topic
import com.viirrtus.queueOverHttp.queue.*
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.AuthorizationException
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.serialization.LongDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.util.concurrent.LinkedBlockingQueue
import java.util.regex.Pattern


/**
 * Adapter implementation over Kafka official client.
 *
 * Main idea is Kafka client are not thread safe. All consumers needs to be
 * handled in separated threads.
 */
class KafkaNativeQueueAdapter(brokerConfig: BrokerConfig) : NativeQueueAdapter<InterruptedKafkaConsumer<String, String>>(brokerConfig) {

    /**
     * Submit new commit request from [queue] for specified [messageNumber].
     */
    override fun commit(queue: PartitionQueue, messageNumber: Long, mode: CommitMode) {
        val kafkaConsumer = consumers.find { it.group.consumer == queue.group.consumer }!!
        (kafkaConsumer as KafkaNativeConsumerWrapper).commit(queue, messageNumber, mode)
    }

    @Throws(ConsumerAlreadyRegisteredException::class)
    override fun subscribe(consumer: Consumer): ConsumerQueueGroup {
        if (isAlreadyRegistered(consumer)) {
            throw ConsumerAlreadyRegisteredException("Same consumer was already found!")
        }

        val properties: MutableMap<String, Any> = brokerConfig.config.toMutableMap()

        //next properties cannot be override by consumer
        properties[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false //key feature
        properties[ConsumerConfig.GROUP_ID_CONFIG] = consumer.group.id
        properties[ConsumerConfig.CLIENT_ID_CONFIG] = consumer.id
        properties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = LongDeserializer::class.java
        properties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java

        val kafkaConsumer = InterruptedKafkaConsumer<String, String>(properties)

        val group = ConsumerQueueGroup(consumer, this)

        // if there is at least one specified partition,
        // we cannot to subscribe using auto balance
        if (consumer.topics.any { it.partitions.isNotEmpty() }) {
            assign(group, kafkaConsumer)
        } else {
            subscribe(group, kafkaConsumer)
        }

        val wrapper = KafkaNativeConsumerWrapper(kafkaConsumer, group)

        Thread(wrapper).start()

        consumers.add(wrapper)

        logger.info("Consumer $consumer successfully registered.")

        return group
    }

    /**
     * Subscribe consumer group to topics with auto balance.
     */
    private fun subscribe(group: ConsumerQueueGroup, kafkaConsumer: KafkaConsumer<String, String>) {
        val rebalanceListener = KafkaRebalanceListener(group)
        val topics = group.consumer.topics

        if (topics.any { it.pattern }) {
            if (topics.size > 1) {
                throw IllegalArgumentException("Pattern subscription are exclusive and available only once per consumer.")
            }
            val pattern = Pattern.compile(topics.first().name)
            kafkaConsumer.subscribe(pattern, rebalanceListener)
        } else {
            kafkaConsumer.subscribe(topics.map { it.name }, rebalanceListener)
        }
    }

    /**
     * Manually assign specified partition to consumer
     */
    private fun assign(group: ConsumerQueueGroup, kafkaConsumer: KafkaConsumer<String, String>) {
        kafkaConsumer.assign(
                group.consumer.topics
                        .flatMap { topic ->
                            topic.partitions.map { TopicPartition(topic.name, it.number.toInt()) }
                        }
        )
    }

    override fun unsubscribe(consumer: Consumer) {
        val kafkaConsumer = consumers.find { it.group.consumer.same(consumer) }
                ?: throw NoConsumerFoundException("No registered consumer found.")

        (kafkaConsumer as KafkaNativeConsumerWrapper).unsubscribe()
    }

    override fun produce(message: Message, topic: Topic) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    companion object {
        private val logger = LoggerFactory.getLogger(this::class.java)
    }

}