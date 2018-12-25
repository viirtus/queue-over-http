package com.viirrtus.queueOverHttp.queue

import com.viirrtus.queueOverHttp.dto.Consumer
import io.reactivex.disposables.CompositeDisposable
import io.reactivex.subjects.BehaviorSubject
import java.util.concurrent.CopyOnWriteArrayList

/**
 * All assigned queue to specific consumer.
 *
 * This is also a transport layer between native adapter and partition queue.
 */
class ConsumerQueueGroup(
        val consumer: Consumer,
        private val nativeQueueAdapter: NativeQueueAdapter<*>
) : MutableCollection<PartitionQueue> {
    private val queues: MutableCollection<PartitionQueue> = CopyOnWriteArrayList<PartitionQueue>()

    private var partitionToQueueMap: Map<LocalTopicPartition, PartitionQueue> = emptyMap()

    override val size: Int
        get() = queues.size

    /**
     * Subject about new queue was assigned to consumer
     */
    val onQueueAddSubject = BehaviorSubject.create<PartitionQueue>()

    /**
     * Subject about queue was revoked from consumer
     */
    val onQueueRemoveSubject = BehaviorSubject.create<PartitionQueue>()

    override fun addAll(elements: Collection<PartitionQueue>): Boolean {
        val result = queues.addAll(elements)
        invalidateSearchMap()

        elements.forEach { onQueueAddSubject.onNext(it) }

        return result
    }

    override fun add(element: PartitionQueue): Boolean {
        val result = queues.add(element)
        invalidateSearchMap()

        onQueueAddSubject.onNext(element)

        return result
    }

    override fun clear() {
        removeAll(queues)
    }

    override fun remove(element: PartitionQueue): Boolean {
        val result = queues.remove(element)
        invalidateSearchMap()
        element.revoke()

        onQueueRemoveSubject.onNext(element)

        return result
    }

    override fun removeAll(elements: Collection<PartitionQueue>): Boolean {
        for (element in elements) {
            element.revoke()
            onQueueRemoveSubject.onNext(element)
        }

        val result = queues.removeAll(elements)
        invalidateSearchMap()

        return result
    }

    override fun retainAll(elements: Collection<PartitionQueue>): Boolean {
        val result = queues.retainAll(elements)
        invalidateSearchMap()

        TODO("Implement operator")
        return result
    }

    override fun contains(element: PartitionQueue): Boolean {
        return queues.contains(element)
    }

    override fun containsAll(elements: Collection<PartitionQueue>): Boolean {
        return queues.containsAll(elements)
    }

    override fun isEmpty(): Boolean {
        return queues.isEmpty()
    }

    override fun iterator(): MutableIterator<PartitionQueue> {
        return queues.iterator()
    }

    /**
     * Rebuild search map from current queues state
     */
    private fun invalidateSearchMap() {
        partitionToQueueMap = queues.associateBy { it.associatedPartition }
    }

    /**
     * Find associated queue with given [partition]
     */
    fun findAssociated(partition: LocalTopicPartition): PartitionQueue? {
        return partitionToQueueMap[partition]
    }

    fun createPartitionQueue(partition: LocalTopicPartition): PartitionQueue {
        return PartitionQueue(consumer, this, partition)
    }

    /**
     * Translate commit request next to native adapter
     */
    fun doCommit(queue: PartitionQueue, offset: Long, mode: CommitMode) {
        nativeQueueAdapter.commit(queue, offset, mode)
    }

}