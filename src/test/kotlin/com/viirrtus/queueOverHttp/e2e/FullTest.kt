package com.viirrtus.queueOverHttp.e2e

import com.fasterxml.jackson.databind.ObjectMapper
import com.viirrtus.queueOverHttp.SpringContextTest
import com.viirrtus.queueOverHttp.controller.TestController
import com.viirrtus.queueOverHttp.dto.Consumer
import com.viirrtus.queueOverHttp.dto.Group
import com.viirrtus.queueOverHttp.dto.Topic
import com.viirrtus.queueOverHttp.originConsumer
import com.viirrtus.queueOverHttp.util.currentTimeMillis
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpEntity
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.test.annotation.DirtiesContext
import org.springframework.web.client.HttpClientErrorException
import org.springframework.web.client.RestTemplate
import java.util.*

/**
 * These tests require active Kafka broker at localhost:9092
 */
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
class FullTest : SpringContextTest() {

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    private lateinit var apiClient: ApiClient

    private val producer: KafkaProducer<String, String>

    init {
        val props = Properties()
//        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "192.168.99.100:9092"
        props[ProducerConfig.CLIENT_ID_CONFIG] = "Producer"
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = LongSerializer::class.java.name
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name

        producer = KafkaProducer(props)

    }

    @Before
    fun init() {
        apiClient = ApiClient(objectMapper)
    }

    @Test()
    fun `test if same consumer cannot be registered twice`() {
        apiClient.subscribe(originConsumer)

        try {
            //this must throw an exception
            apiClient.subscribe(originConsumer)
            Assert.assertTrue(false)
        } catch (e: HttpClientErrorException.BadRequest) {}

        apiClient.unsubscribe(originConsumer)
    }

    @Test
    fun `test if consumers can be registered and unsubscribed`() {
        val consumers = (0..10).map { originConsumer.copy(id = "consumer-$it", group = Group("group-$it")) }

        //subscribe each
        for (consumer in consumers) {
            apiClient.subscribe(consumer)
        }

        val listedConsumers = apiClient.listSubscriptions()

        //check if we get all subscribed consumers
        Assert.assertTrue(listedConsumers.isNotEmpty())
        for (consumer in consumers) {
            val responseConsumer = listedConsumers.find { it.same(consumer) }
            Assert.assertNotNull(responseConsumer)
        }

        val unsubscribedConsumer = consumers.first()
        apiClient.unsubscribe(unsubscribedConsumer)

        val listedConsumersNew = apiClient.listSubscriptions()

        //check if unsubscribed consumer no more in list
        Assert.assertTrue(listedConsumersNew.isNotEmpty())
        for (consumer in listedConsumersNew) {
            Assert.assertFalse(consumer.same(unsubscribedConsumer))
        }

        for (consumer in listedConsumersNew) {
            apiClient.unsubscribe(consumer)
        }
    }


    @Test
    fun `test if subscribed consumer received messages`() {
        val consumers = (0..10).map { originConsumer.copy(id = "consumer-$it", group = Group("group-$it")) }

        //subscribe each
        for (consumer in consumers) {
            apiClient.subscribe(consumer)
        }

        //wait until rebalance happened
        Thread.sleep(5000)

        //let's produce [messageCount] message to each topic
        val messageCount = 100
        repeat(messageCount) {
            produceMessage("test_$it", originConsumer.topics)
        }

        //wait until all messages will be received by consumers or timeout
        val startWaitTime = currentTimeMillis()
        while (
                TestController.receivedMessages.size < consumers.size * originConsumer.topics.size * messageCount
                && currentTimeMillis() - startWaitTime < 2000 * messageCount
        ) {
            Thread.sleep(10)
        }

        Assert.assertEquals(consumers.size * originConsumer.topics.size * messageCount, TestController.receivedMessages.size)

        for (consumer in consumers) {
            //each consumer must receive own part
            val consumedMessages = TestController.receivedMessages.filter { it.consumerHeader == consumer.toTinyString() }
            Assert.assertEquals(originConsumer.topics.size * messageCount, consumedMessages.size)

            //and for each topic
            val messagesGroupedByTopic = consumedMessages.groupBy { it.topicHeader }
            Assert.assertEquals(originConsumer.topics.size, messagesGroupedByTopic.size)

            //and only dispatched count of messages for each topic
            messagesGroupedByTopic.forEach { topic, messages ->
                Assert.assertEquals(messageCount, messages.size)

                //and without duplicates
                val hasDuplicates = !messages.groupingBy { it.numberHeader }
                        .eachCount()
                        .none { it.value > 1 }
                Assert.assertFalse(hasDuplicates)
            }
        }
    }


    private fun produceMessage(message: String, topics: List<Topic>) {
        for (topic in topics) {
            producer.send(ProducerRecord(topic.name, message))
        }
        producer.flush()
    }

}

fun ClosedRange<Char>.randomString(lenght: Int) =
        (1..lenght)
                .map { (Random().nextInt(endInclusive.toInt() - start.toInt()) + start.toInt()).toChar() }
                .joinToString("")


fun <T> ObjectMapper.readResponseArray(body: String, resultClass: Class<T>): List<T> {
    val tree = readTree(body)
    val resultNode = tree.path("data")

    return resultNode.map { treeToValue(it, resultClass) }
}

class ApiClient(private val objectMapper: ObjectMapper) {
    private val apiLocation = "http://localhost:8080"
    private val httpClient = RestTemplate()

    fun subscribe(consumer: Consumer) {
        val response = httpClient.exchange("$apiLocation/broker/subscription", HttpMethod.POST, HttpEntity(consumer), String::class.java)
        Assert.assertEquals(HttpStatus.OK, response.statusCode)
    }

    fun listSubscriptions(): List<Consumer> {
        val response = httpClient.getForEntity("$apiLocation/broker/subscription", String::class.java)
        return objectMapper.readResponseArray(response.body!!, Consumer::class.java)
    }

    fun unsubscribe(consumer: Consumer) {
        val response = httpClient.exchange("$apiLocation/broker/unsubscribe", HttpMethod.POST, HttpEntity(consumer), String::class.java)
        Assert.assertEquals(HttpStatus.OK, response.statusCode)
    }
}
