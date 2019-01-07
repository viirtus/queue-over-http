package com.viirrtus.queueOverHttp.service

import com.viirrtus.queueOverHttp.MockDispatchTask
import com.viirrtus.queueOverHttp.QueueDataMock
import com.viirrtus.queueOverHttp.any
import com.viirrtus.queueOverHttp.queue.PartitionQueue
import com.viirrtus.queueOverHttp.queue.populate
import com.viirrtus.queueOverHttp.service.task.TaskExecutorService
import org.junit.Assert
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Mockito.*
import org.mockito.junit.MockitoJUnitRunner
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger


@RunWith(MockitoJUnitRunner::class)
class QueueDrainerTest {

    private var mock = QueueDataMock()

    @Test
    fun `test if new queues are drained fine`() {
        val executor = mock(TaskExecutorService::class.java)
        val drainer = QueueDrainer(executor)

        val queues = (0..10).map { mock.newQueue }
        val group = mock.queueGroup

        drainer.add(group)

        group.addAll(queues)

        val drainerThread = Thread(drainer)
        drainerThread.start()

        val awaitedMessageCount = mock.topic.config.concurrencyFactor
        val latch = CountDownLatch(awaitedMessageCount * queues.size)
        val consumeMap = ConcurrentHashMap<PartitionQueue, AtomicInteger>()
        `when`(executor.submit(any(), anyLong())).then {
            val task = it.getArgument<MockDispatchTask<*>>(0)
            val message = task.capturedMessage
            val counter = consumeMap.computeIfAbsent(message.queue) { AtomicInteger(0) }
            counter.incrementAndGet()

            latch.countDown()

            null
        }

        for (queue in queues) {
            queue.populate(awaitedMessageCount)
        }

        latch.await(10, TimeUnit.SECONDS)

        Assert.assertEquals(queues.size, consumeMap.size)

        consumeMap.forEach { k, v ->
            Assert.assertEquals(v.get(), awaitedMessageCount)
        }
    }

}
