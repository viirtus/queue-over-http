package com.viirrtus.queueOverHttp

import com.viirrtus.queueOverHttp.dto.*
import com.viirrtus.queueOverHttp.queue.*
import com.viirrtus.queueOverHttp.service.task.DispatchTask
import com.viirrtus.queueOverHttp.service.task.TaskExecutorService
import org.mockito.Mockito
import org.mockito.Mockito.mock
import org.springframework.http.HttpMethod
import java.util.concurrent.CompletableFuture

val originConsumer = Consumer(
        id = "test-consumer",
        group = Group(
                id = "test-group"
        ),
        subscriptionMethod = HttpSubscriptionMethod(
                uri = "http://localhost:8080/test"
        ),
        broker = "Test-Broker",
        topics = (0..9).map {
            Topic(
                    name = "test${Math.random()}_$it",
                    config = Config(concurrencyFactor = 500)
            )
        }
)

val testConsumer = originConsumer.copy(
        subscriptionMethod = MockHttpSubscriptionMethod(
                uri = "http://localhost:8080/test"
        ),
        topics = (0..10).map {
            Topic(
                    name = "test_$it",
                    config = Config(concurrencyFactor = 500)
            )
        }
)

class QueueDataMock() {
    val nativeQueueAdapter: NativeQueueAdapter<*> = mock(NativeQueueAdapter::class.java)
    val queueGroup = ConsumerQueueGroup(testConsumer, nativeQueueAdapter)
    val topic = testConsumer.topics.first()
    val queue = PartitionQueue(queueGroup, LocalTopicPartition(topic.name, "0"))

    private var queueCounter = 0
    val newQueue: PartitionQueue
        get() = PartitionQueue(queueGroup, LocalTopicPartition("test_${queueCounter++}", "0"))
}

class MockHttpSubscriptionMethod(
        delayOnErrorMs: Long = 0,
        retryBeforeCommit: Long = 0,
        uri: String = "",
        method: HttpMethod = HttpMethod.POST,
        queryParamKey: String = "message",
        charset: String = "UTF-8",
        additionalHeaders: Map<String, String> = emptyMap()
) : HttpSubscriptionMethod(delayOnErrorMs, retryBeforeCommit, uri, method, queryParamKey, charset, additionalHeaders) {
    override fun createTask(message: AssociatedMessage, executorService: TaskExecutorService): DispatchTask<*> {
        val originalTask = super.createTask(message, executorService)

        return MockDispatchTask(originalTask, message, executorService)
    }
}

class MockDispatchTask<T>(
        val originalTask: DispatchTask<T>,
        message: AssociatedMessage,
        executorService: TaskExecutorService
) : DispatchTask<T>(message, executorService) {
    val capturedMessage = message

    override fun execute(): CompletableFuture<T> = originalTask.execute()

}

fun <T> eq(obj: T): T = Mockito.eq<T>(obj)
fun <T> any(): T = Mockito.any<T>()