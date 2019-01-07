package com.viirrtus.queueOverHttp.queue

import com.viirrtus.queueOverHttp.QueueDataMock
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Mockito.mock
import org.mockito.junit.MockitoJUnitRunner

@RunWith(MockitoJUnitRunner::class)
class ConsumerQueueGroupTest {
    private lateinit var queueGroup: ConsumerQueueGroup

    @Before
    fun initMock() {
        val mockData = QueueDataMock()
        queueGroup = mockData.queueGroup
    }

    @Test
    fun `test if group notify subscribers about changes`() {
        val queue = mock(PartitionQueue::class.java)

        var addExpected = true
        var addInvokedCounter = 0
        queueGroup.onQueueAddSubject.subscribe() {
            Assert.assertTrue(addExpected)
            addInvokedCounter++
        }

        var removeExpected = false
        var removeInvokedCounter = 0
        queueGroup.onQueueRemoveSubject.subscribe() {
            Assert.assertTrue(removeExpected)
            removeInvokedCounter++
        }
        queueGroup.add(queue)

        Assert.assertEquals(1, addInvokedCounter)

        val expectedQueueCount = 10
        val queues = (0..expectedQueueCount).map { mock(PartitionQueue::class.java) }
        queueGroup.addAll(queues)

        Assert.assertEquals(expectedQueueCount + 2, addInvokedCounter)

        removeExpected = true
        queueGroup.remove(queue)

        Assert.assertEquals(1, removeInvokedCounter)

        queueGroup.removeAll(queues)
        Assert.assertEquals(expectedQueueCount + 2, removeInvokedCounter)

        Assert.assertEquals(0, queueGroup.size)
    }
}