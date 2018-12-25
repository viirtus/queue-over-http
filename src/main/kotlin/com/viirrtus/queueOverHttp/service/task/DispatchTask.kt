package com.viirrtus.queueOverHttp.service.task

import com.viirrtus.queueOverHttp.Application
import com.viirrtus.queueOverHttp.queue.AssociatedMessage
import com.viirrtus.queueOverHttp.queue.PartitionQueueStatus
import com.viirrtus.queueOverHttp.util.currentTimeMillis
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.function.BiConsumer

/**
 * [T] - result of task execution
 */
abstract class DispatchTask<T>(
        /**
         * Message to de dispatched
         */
        protected val message: AssociatedMessage,

        /**
         * Executors for running task
         */
        private val executorService: TaskExecutorService
) : Runnable {
    /**
     * How many times task was fail
     */
    @Volatile
    protected var retryCount = 0

    /**
     * Current dispatching method
     */
    protected val subscriptionMethod = message.consumer.subscriptionMethod

    /**
     * Task execution time start.
     */
    private var startTimeMs: Long = 0

    /**
     * Task completion time
     */
    private var endTimeMs: Long = 0

    override fun run() {
        startTimeMs = currentTimeMillis()

        val future = execute()
        val consumer = BiConsumer<T, Throwable?> { m, e ->
            if (e == null) {
                onTaskCompleted(m)
            } else {
                onTaskFail(e)
            }
        }

        executorService.submit(future, consumer)
    }

    /**
     * Execute task.
     * If there is any blocks, task must be divided into
     * multiple executions.
     * Result of execution must be represented by CompletableFuture
     */
    abstract fun execute(): CompletableFuture<T>

    /**
     * When task successfully done.
     */
    protected open fun onTaskCompleted(result: T) {
        if (Application.DEBUG) {
            logger.debug("Task $this successfully completed after ${retryCount + 1} attempts in ${elapsedTimeMs()} ms.")
        }
        onComplete()
    }

    /**
     * When task failed, but cannot be retried.
     */
    protected open fun onFailedTaskCompleted() {
        if (Application.DEBUG) {
            logger.debug("Task $this failed after $retryCount attempts in ${elapsedTimeMs()} ms.")
        }
        onComplete()
    }

    /**
     * Final stage of task execution.
     * Assigned message will be commited.
     */
    private fun onComplete() {
        endTimeMs = currentTimeMillis()
        commit()
    }

    /**
     * On task failed due to some code exception and must be retried
     */
    protected open fun onTaskFail(throwable: Throwable) {
        logger.error("Task $this failed (retryCount=$retryCount, elapsedTime=${elapsedTimeMs()} ms)", throwable)
        retry()
    }

    /**
     * When task completed by error on consumer side.
     * Task will be retried if there is left attemts,
     * otherwise, task complete failed.
     */
    protected fun onTaskError() {
        if (subscriptionMethod.retryBeforeCommit == 0L || retryCount in 0..subscriptionMethod.retryBeforeCommit) {
            retry(subscriptionMethod.delayOnErrorMs)
        } else {
            onFailedTaskCompleted()
        }
    }

    /**
     * Retry this task.
     * Optionally, delay before next execution can be provided.
     *
     * There is unconditional delay before retry - 1ms - for
     * prevent high CPU usage.
     */
    private fun retry(delay: Long = 0) {
        if (message.queue.status == PartitionQueueStatus.REVOKED) {
            if (Application.DEBUG) {
                logger.debug("Aborting task $this retry since origin queue was revoked.")
            }
            return
        }

        retryCount++

        if (Application.DEBUG) {
            logger.debug("Retry task $this after error or fail with $delay ms delay (retryCount=$retryCount).")
        }
        executorService.submit(this, delay + 1)
    }

    private fun commit() {
        message.commit()
    }

    /**
     * Time since first task execution.
     */
    private fun elapsedTimeMs(): Long {
        return currentTimeMillis() - startTimeMs
    }

    override fun toString(): String {
        return "${this::class.java.simpleName}[$message]"
    }

    companion object {
        private val logger = LoggerFactory.getLogger(DispatchTask::class.java)
    }
}


