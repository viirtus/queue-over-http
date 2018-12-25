package com.viirrtus.queueOverHttp.service.task

import org.springframework.stereotype.Component
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.function.BiConsumer

@Component
class TaskExecutorService {
    /**
     * For regular task execution.
     */
    private val executorsPool = Executors.newWorkStealingPool()

    /**
     * For delaying before task execution.
     */
    private val schedulingPool = Executors.newSingleThreadScheduledExecutor()

    /**
     * Execute task in pools.
     * If delay specified, task execution will be delayed
     * by specified time.
     *
     * Tasks are always executed in [executorsPool].
     *
     */
    fun submit(task: DispatchTask<*>, delayMs: Long = 0) {
        if (delayMs == 0L) {
            executorsPool.submit(task)
        } else {
            schedulingPool.schedule({
                executorsPool.submit(task)
            }, delayMs, TimeUnit.MILLISECONDS)
        }
    }

    /**
     * Async handling future completion.
     */
    fun <T> submit(future: CompletableFuture<T>, callback: BiConsumer<T, Throwable?>) {
        future.whenCompleteAsync(callback, executorsPool)
    }
}