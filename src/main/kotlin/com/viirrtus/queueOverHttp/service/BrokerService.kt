package com.viirrtus.queueOverHttp.service

import com.viirrtus.queueOverHttp.Application
import com.viirrtus.queueOverHttp.config.AppConfig
import com.viirrtus.queueOverHttp.dto.Consumer
import com.viirrtus.queueOverHttp.queue.NativeQueueAdapter
import com.viirrtus.queueOverHttp.queue.PartitionQueueStatus
import com.viirrtus.queueOverHttp.service.exception.IllegalConsumerConfigException
import com.viirrtus.queueOverHttp.service.persistence.PersistenceAdapterInterface
import com.viirrtus.queueOverHttp.util.currentTimeMillis
import io.reactivex.disposables.CompositeDisposable
import io.reactivex.subjects.BehaviorSubject
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import javax.annotation.PostConstruct
import kotlin.reflect.full.primaryConstructor

/**
 * Entry point for manipulation across all queues.
 */
@Service
class BrokerService(
        val config: AppConfig,
        private val queueDrainerBalancer: DrainerBalancer,
        private val persistenceAdapter: PersistenceAdapterInterface
) {
    private val unconditionalCommitTimeoutMs = 1_000L

    private val queues: Map<String, NativeQueueAdapter<*>> = config.brokers
            .associateBy { it.name }
            .mapValues {
                val queueClass = it.value.origin.clazz
                queueClass.primaryConstructor!!.call(it.value)
            }

    @PostConstruct
    fun init() {
        Runtime.getRuntime().addShutdownHook(Thread {
            Thread.currentThread().name = "Shutdowner"
            shutdown()
        })

        restore()
    }

    private fun restore() {
        val storedConsumers = persistenceAdapter.restore()
        for (consumer in storedConsumers) {
            subscribe(consumer, SubscribeMode.RESTORE)
        }
    }

    /**
     * Do subscribe.
     * Consumer will be registered in requested broker if
     * it exists.
     *
     */
    fun subscribe(consumer: Consumer, mode: SubscribeMode = SubscribeMode.NORMAL) {
        val associatedManipulator = queues[consumer.broker]
                ?: throw IllegalConsumerConfigException("No brokers with name=${consumer.broker} found.")

        try {
            val group = associatedManipulator.subscribe(consumer)
            queueDrainerBalancer.add(group)

            if (mode == SubscribeMode.NORMAL) {
                persistenceAdapter.store(consumer)
            }
            logger.info("Consumer $consumer registered.")
        } catch (e: Exception) {
            throw IllegalConsumerConfigException("Cannot register consumer: ${e.message}")
        }
    }

    /**
     * Grace unsubscribe action.
     */
    fun unsubscribe(consumer: Consumer, timeout: Long, timeUnit: TimeUnit) {
        val timeStart = currentTimeMillis()

        val associatedManipulator = shutdown(consumer, timeout, timeUnit)
        associatedManipulator.unsubscribe(consumer)

        persistenceAdapter.remove(consumer)
        logger.info("Unsubscribe $consumer in ${currentTimeMillis() - timeStart} ms")
    }

    /**
     * Stop dispatching and consuming messages from
     * associated queues.
     *
     * We must wait until already dispatched but not
     * completed task complete or timeout
     * and try to commit pending messages.
     */
    private fun shutdown(consumer: Consumer, timeout: Long, timeUnit: TimeUnit): NativeQueueAdapter<*> {
        val timeStart = currentTimeMillis()
        val associatedManipulator = queues[consumer.broker]
                ?: throw IllegalConsumerConfigException("No brokers with name=${consumer.broker} found.")

        val group = associatedManipulator.findGroup(consumer)
                ?: throw IllegalConsumerConfigException("Consumer not registered yet")

        logger.info("Do shutdown for $consumer")

        val queues = group.toList()
        group.clear()
        queueDrainerBalancer.remove(group)

        val timeoutMillis = timeUnit.toMillis(timeout)

        var ready = false
        // waiting until each consumer queue
        // becomes ready to die, i.e. all consumed messages
        // becomes completed
        while (!Thread.interrupted()
                && currentTimeMillis() - timeStart < timeoutMillis
        ) {
            ready = queues.all { it.status == PartitionQueueStatus.READY_TO_DIE }
            if (ready) {
                break
            }

            for (queue in queues) {
                queue.commitPending()

                if (Application.DEBUG) {
                    logger.debug("Queue $queue state: ${queue.stats}")
                }
            }

            logger.info("Waiting until all queues becomes ready to die...")

            Thread.sleep(100)
        }

        if (!ready) {
            logger.info("Some queues are locked in waiting for completed tasks. Try to commit already completed with commit waiting.")
            // Last resort for not losing any data
            val barrier = CountDownLatch(group.size)
            val disposable = queues
                    .map {
                        val subject = BehaviorSubject.create<Long>()
                        subject to it.commitPending(subject)
                    }
                    .map {
                        val subject = it.first
                        val awaitedCommitNumber = it.second

                        subject.subscribe { messageNumber ->
                            if (messageNumber == -1L || messageNumber == awaitedCommitNumber) {
                                barrier.countDown()
                            }
                        }
                    }
                    .fold(CompositeDisposable()) { container, d ->
                        container.add(d)

                        container
                    }

            val timeLeftMs = Math.max(unconditionalCommitTimeoutMs, currentTimeMillis() - timeStart)
            val commitOk = barrier.await(timeLeftMs, TimeUnit.MILLISECONDS)
            if (!commitOk) {
                logger.warn("Pending messages commit timeout of $consumer. Some data may be lost.")
            }

            disposable.dispose()
        }

        logger.info("Shutdown of $consumer completed.")

        group.clear()

        return associatedManipulator
    }

    /**
     * Shutdown all registered consumers.
     */
    fun shutdown() {
        logger.info("Start graceful shutdown...")

        val consumers = list()
        for (consumer in consumers) {
            shutdown(consumer, 10, TimeUnit.SECONDS)
        }

        logger.info("Shutdown complete!")
    }

    /**
     * List all registered consumers across all queues.
     */
    fun list(): List<Consumer> {
        return queues.values.flatMap { it.listConsumers() }
    }

    enum class SubscribeMode {
        NORMAL,
        RESTORE
    }

    companion object {
        private val logger = LoggerFactory.getLogger(this::class.java)
    }
}