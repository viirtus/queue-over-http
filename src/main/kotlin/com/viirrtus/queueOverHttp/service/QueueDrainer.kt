package com.viirrtus.queueOverHttp.service

import com.viirrtus.queueOverHttp.Application
import com.viirrtus.queueOverHttp.queue.ConsumerQueueGroup
import com.viirrtus.queueOverHttp.queue.PartitionQueue
import com.viirrtus.queueOverHttp.queue.PartitionQueueStatus
import com.viirrtus.queueOverHttp.service.task.TaskExecutorService
import com.viirrtus.queueOverHttp.util.StatefulLatch
import com.viirrtus.queueOverHttp.util.currentTimeMillis
import com.viirrtus.queueOverHttp.util.hiResCurrentTimeNanos
import io.reactivex.disposables.CompositeDisposable
import io.reactivex.disposables.Disposable
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

/**
 * Bridge between consumer queues and actual tasks execution.
 *
 * Working set - is a partition queues that drained by this instance.
 */
@Component
class QueueDrainer(
        private val executorService: TaskExecutorService
) : Runnable, QueueDrainerInterface {
    private val queueTimeQuantMs = 1L

    private val queueTimeQuantNanos = queueTimeQuantMs * 1_000_000

    private val unconditionalFetchSize = 100

    private val groupToDisposableMap = ConcurrentHashMap<ConsumerQueueGroup, Disposable>()

    /**
     * Modification over [queues] is done only in main loop.
     * Add and remove operations needs to be synchronized both
     * for reflecting in queues atomically.
     */
    private val lock = Semaphore(2)

    private val latch = StatefulLatch(1)

    private val newQueues = mutableListOf<PartitionQueue>()

    private val revokedQueues = mutableListOf<PartitionQueue>()

    /**
     * Current working set
     */
    private val queues = mutableListOf<PartitionQueue>()

    /**
     * Register group to be drained by this drainer.
     * All new queues automatically goes to working set.
     * All revoking queue removes from working set.
     */
    override fun add(consumerQueueGroup: ConsumerQueueGroup) {
        val disposableContainer = CompositeDisposable()
        disposableContainer.addAll(
                consumerQueueGroup.onQueueAddSubject
                        .subscribe(this::onNewQueueFound),
                consumerQueueGroup.onQueueRemoveSubject
                        .subscribe(this::onQueueRevoked)
        )

        groupToDisposableMap[consumerQueueGroup] = disposableContainer
    }

    override fun remove(consumerQueueGroup: ConsumerQueueGroup) {
        val disposable = groupToDisposableMap.remove(consumerQueueGroup)
                ?: return

        disposable.dispose()
        consumerQueueGroup.forEach { onQueueRevoked(it) }
    }

    /**
     * Stash new queue until main loop take it.
     */
    private fun onNewQueueFound(queue: PartitionQueue) {
        lock.acquire()
        queue.owner = this
        newQueues.add(queue)
        lock.release()
    }

    /**
     * Stash removed queue until main loop take it.
     */
    private fun onQueueRevoked(queue: PartitionQueue) {
        lock.acquire()
        queue.owner = null
        revokedQueues.add(queue)
        lock.release()
    }

    /**
     * Countdown on latch.
     * If drainer are locked at latch, it will notify he for
     * processing arrived messages.
     */
    override fun onAction(queue: PartitionQueue) {
        latch.countDown()
    }

    /**
     * Iterates over working set lineally.
     * Each queue will be treated for [queueTimeQuantNanos] if
     * there is any messages in.
     */
    override fun run() {
        Thread.currentThread().name = "QueueDrainer-${drainerId++}"

        while (!Thread.interrupted()) {
            val latchStartState = latch.currentState

            var wasWork = false

            //low produce rate queues goings first for minimize latency
            //we must capture current state befor sorting to avoid sort exception
            val sortMap = queues.associateWith { it.size }
            queues.sortBy { sortMap[it] }
            for (queue in queues) {
                val startTimeNanos = hiResCurrentTimeNanos()
                while (true) {
                    val leftTimeNanos = queueTimeQuantNanos - hiResCurrentTimeNanos() + startTimeNanos
                    val status = queue.status
                    if (leftTimeNanos < 0
                            || status == PartitionQueueStatus.EMPTY
                            || status == PartitionQueueStatus.LOCKED
                            || status == PartitionQueueStatus.REVOKED
                    ) {
                        break
                    }

                    //optimization for reduce status counting on each iteration
                    for (i in 0 until unconditionalFetchSize) {
                        val assocMessage = queue.pop() ?: break
                        val task = assocMessage.toTask(executorService)

                        if (Application.DEBUG) {
                            logger.debug("Submit task $task for $queue")
                        }

                        executorService.submit(task)

                        wasWork = true
                    }
                }

                if (queue.config.autoCommitPeriodMs > 0L) {
                    val msSinceLastCommit = currentTimeMillis() - queue.lastCommitTimeMs

                    if (msSinceLastCommit >= queue.config.autoCommitPeriodMs) {
                        queue.commitPending()
                    }
                }
            }

            // nothing works and there is no latch state
            // changes while last processing
            if (!wasWork && latch.tryReset(latchStartState)) {
                latch.await(1, TimeUnit.MILLISECONDS)
            }

            invalidateQueues()
        }
    }

    /**
     * Do adding pending new queues and
     * removing revoked.
     */
    private fun invalidateQueues() {
        lock.acquire(2)

        if (newQueues.isNotEmpty()) {
            queues.addAll(newQueues)
            newQueues.clear()
        }

        if (revokedQueues.isNotEmpty()) {
            queues.removeAll(revokedQueues)
            revokedQueues.clear()
        }

        lock.release(2)
    }

    companion object {
        private var drainerId = 0

        private val logger = LoggerFactory.getLogger(this::class.java)
    }
}