package com.viirrtus.queueOverHttp.queue

import com.viirrtus.queueOverHttp.QueueDataMock
import com.viirrtus.queueOverHttp.dto.Topic
import com.viirrtus.queueOverHttp.service.QueueDrainerInterface
import com.viirrtus.queueOverHttp.util.hiResCurrentTimeNanos
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers.anyLong
import org.mockito.Mockito
import org.mockito.Mockito.`when`
import org.mockito.Mockito.mock
import org.mockito.junit.MockitoJUnitRunner
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors

@RunWith(MockitoJUnitRunner::class)
class PartitionQueueTest {

    private lateinit var nativeQueueAdapter: NativeQueueAdapter<*>
    private lateinit var topic: Topic
    private lateinit var queue: PartitionQueue

    @Before
    fun initMock() {
        val mockData = QueueDataMock()

        topic = mockData.topic
        queue = mockData.queue
        nativeQueueAdapter = mockData.nativeQueueAdapter
    }

    @Test
    fun `test if queue forbid consuming more than concurrency factor`() {

        queue.populate(topic.config.concurrencyFactor)

        for (i in 0 until topic.config.concurrencyFactor) {
            val message = queue.pop()
            Assert.assertNotNull(message)
        }

        val nextNullMessage = queue.pop()

        Assert.assertNull(nextNullMessage)
        Assert.assertTrue(queue.size > 0)
        Assert.assertEquals(PartitionQueueStatus.LOCKED, queue.status)
    }

    @Test
    fun `test if queue are becomes ready to die only when all tasks committed`() {
        queue.populate(topic.config.concurrencyFactor - 1)
        Assert.assertEquals(PartitionQueueStatus.READY, queue.status)

        queue.drainAvailable()
        Assert.assertEquals(PartitionQueueStatus.EMPTY, queue.status)

        queue.revoke()
        queue.onAfterCommit((topic.config.concurrencyFactor - 2).toLong(), CommitMode.NORMAL)
        Assert.assertEquals(PartitionQueueStatus.REVOKED, queue.status)

        queue.onAfterCommit((topic.config.concurrencyFactor - 1).toLong(), CommitMode.NORMAL)
        Assert.assertEquals(PartitionQueueStatus.READY_TO_DIE, queue.status)

        Assert.assertEquals(null, queue.pop())
    }

    @Test
    fun `test if commit triggered when batch completed`() {
        queue.populate(topic.config.concurrencyFactor - 1)

        var isReady = false
        var commitInvoked = false
        val expectedCommitThread = Thread.currentThread()
        `when`(nativeQueueAdapter.commit(eq(queue), anyLong(), eq(CommitMode.NORMAL))).then {
            Assert.assertEquals(expectedCommitThread, Thread.currentThread())
            commitInvoked = true
            Assert.assertTrue(isReady)
        }

        val exec = Executors.newCachedThreadPool()

        val latch = CountDownLatch(topic.config.concurrencyFactor - 1)
        for (i in 1 until topic.config.concurrencyFactor) {
            exec.execute {
                queue.commit(i.toLong())
                latch.countDown()
            }
        }

        latch.await()

        isReady = true
        queue.commit(0)

        Assert.assertTrue(commitInvoked)
    }

    @Test(expected = RuntimeException::class)
    fun `test if cannot commit message outside consume range`() {
        queue.populate(topic.config.concurrencyFactor - 1)
        queue.drainAvailable()

        queue.commit(topic.config.concurrencyFactor.toLong())
    }

    @Test
    fun `test if commit pending are strongly ordered`() {
        queue.populate(topic.config.concurrencyFactor - 1)
        queue.drainAvailable()
        queue.commitRange(0, topic.config.concurrencyFactor / 2)

        var isReady = true
        var commitInvoked = false
        var expectedCommitNumber = (topic.config.concurrencyFactor / 2) - 1
        `when`(nativeQueueAdapter.commit(eq(queue), anyLong(), any())).then {
            Assert.assertTrue(isReady)
            commitInvoked = true
            Assert.assertEquals(expectedCommitNumber, (it.getArgument(1) as Long).toInt())
        }

        queue.commitPending()
        Assert.assertTrue(commitInvoked)

        isReady = false
        // leave hole in a middle and leave last message
        queue.commitRange(topic.config.concurrencyFactor / 2 + 1, topic.config.concurrencyFactor - 1)

        // nothing must be committed, there is a hole
        queue.commitPending()

        // commit hole in a middle
        queue.commit((topic.config.concurrencyFactor / 2).toLong())

        isReady = true
        commitInvoked = false
        //last message not ready yet
        expectedCommitNumber = topic.config.concurrencyFactor - 2

        queue.commitPending()
        Assert.assertTrue(commitInvoked)
    }

    @Test
    fun `test if owner notified`() {
        var actionExpected = false
        var actionInvoked = false
        val drainer = mock(QueueDrainerInterface::class.java)
        `when`(drainer.onAction(eq(queue))).then {
            actionInvoked = true
            Assert.assertTrue(actionExpected)
        }
        queue.owner = drainer

        actionExpected = true
        //populating must trigger notification
        queue.populate(topic.config.concurrencyFactor * 2)
        Assert.assertTrue(actionInvoked)

        actionInvoked = false
        actionExpected = false

        queue.drainAvailable()

        actionExpected = true
        queue.onAfterCommit(topic.config.concurrencyFactor.toLong() - 1, CommitMode.NORMAL)
        Assert.assertTrue(actionInvoked)

        actionInvoked = false
        queue.onCommitFailed(0, CommitMode.NORMAL, Exception())
        Assert.assertTrue(actionInvoked)

    }
}

fun PartitionQueue.populate(messagesCount: Int) {
    push((0..messagesCount).map { Message("", it.toLong(), null, hiResCurrentTimeNanos()) })
}

fun PartitionQueue.drainAvailable() {
    while (true) {
        if (pop() == null) break
    }
}

fun PartitionQueue.commitRange(from: Int, to: Int) {
    for (i in from until to) {
        commit(i.toLong())
    }
}

fun <T> eq(obj: T): T = Mockito.eq<T>(obj)
fun <T> any(): T = Mockito.any<T>()