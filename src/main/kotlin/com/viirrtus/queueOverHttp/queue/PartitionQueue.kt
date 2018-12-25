package com.viirrtus.queueOverHttp.queue

import com.viirrtus.queueOverHttp.Application
import com.viirrtus.queueOverHttp.dto.Consumer
import com.viirrtus.queueOverHttp.service.QueueDrainerInterface
import com.viirrtus.queueOverHttp.util.currentTimeMillis
import com.viirrtus.queueOverHttp.util.hiResCurrentTimeNanos
import io.reactivex.subjects.Subject
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLongArray
import java.util.concurrent.locks.ReentrantLock

/**
 * Queue over concrete topic partition.
 * There is tree basic operation:
 * 1. push - receive messages from native queue
 * 2. pop - give message
 * 3. commit - mark message as completed
 *
 * This class are fully thread safe.
 *
 * General idea: drain queue only if consumer
 * concurrency factor allows it.
 *
 * Handling completed messages realized with bit mask [commitedMask]
 * with size equals to [consumeFactor].
 * Each compleated message set a corresponding to message number bit in mask.
 * If mask a fully filled, i.e. all bit are set, all messages from this chunk
 * was compleated and queue perform commit request to native queue.
 * While chunk not commited, new messages cannot be consumed.
 */
class PartitionQueue(
        /**
         * Consumer of this queue
         */
        private val consumer: Consumer,

        /**
         * Holder
         */
        val group: ConsumerQueueGroup,

        /**
         * Partition from which messages came
         */
        val associatedPartition: LocalTopicPartition
) {
    val config = consumer.topics.find { it.matched(associatedPartition) }!!.config

    /**
     * Time of last commit in milliseconds
     */
    var lastCommitTimeMs = 0L
        private set

    /**
     * Is queue was paused?
     * This is a form of back pressure, paused queue
     * will not consume any message from native queue.
     */
    var isPaused: Boolean = false
        private set

    /**
     * Current drainer of this queue
     */
    var owner: QueueDrainerInterface? = null

    val size: Int
        get() = queue.size

    /**
     * Show that queue was revoked. Since revoke,
     * messages cannot be consumed from this queue.
     */
    @Volatile
    private var isRevoked = false

    @Volatile
    private var lastConsumedMessageNumber = 0L

    @Volatile
    private var lastNormalCommitMessageNumber = 0L

    private var lastCommitMessageNumber = 0L

    private var lastForceCommitRequestMessageNumber = 0L

    /**
     * In general, listeners for waiting while commit
     * in unsubscribe action.
     */
    private val commitListeners = CopyOnWriteArrayList<Subject<Long>>()

    /**
     * Original message queue holder
     */
    private val queue: LinkedBlockingQueue<Message> = LinkedBlockingQueue()

    /**
     * A load factor indicate max size of [queue] over which
     * queue cannot consume more items.
     */
    private val loadFactor = config.concurrencyFactor * 2

    /**
     * Max items that can be consumed from queue since
     * last normal commit
     */
    private val consumeFactor = config.concurrencyFactor


    private val commitLock = ReentrantLock()

    private var isFistMessageInit = false

    private val longWord = -1L

    private val longWordSize = 64

    /**
     * Mask for marking completed messages.
     */
    private val commitedMask = AtomicLongArray((consumeFactor / longWordSize) + 1)

    /**
     * A reference for commit mask. If mask are equals to reference,
     * all messages from batch since last normal commit are completed.
     */
    private val referenceMaskForCommit = LongArray(commitedMask.length())

    private val awaitedReferenceLength = consumeFactor / longWordSize

    init {
        //fill first [consumeFactor] bits in reference
        val awaitedWordBitIndex = consumeFactor % longWordSize

        for (i in (0 until awaitedReferenceLength)) {
            referenceMaskForCommit[i] = longWord
        }

        if (awaitedWordBitIndex > 0) {
            referenceMaskForCommit[awaitedReferenceLength] = longWord.shl(longWordSize - awaitedWordBitIndex)
        }
    }

    /**
     * Current queue status
     */
    val status: PartitionQueueStatus
        get() {
            val size = queue.size
            return when {
                isRevoked && lastCommitMessageNumber == lastConsumedMessageNumber -> PartitionQueueStatus.READY_TO_DIE
                isRevoked -> PartitionQueueStatus.REVOKED
                size == 0 -> PartitionQueueStatus.EMPTY
                size >= loadFactor -> PartitionQueueStatus.FULL
                lastConsumedMessageNumber - lastNormalCommitMessageNumber >= consumeFactor -> PartitionQueueStatus.LOCKED
                else -> PartitionQueueStatus.READY
            }
        }

    /**
     * Exposing inner stats for current queue state
     */
    val stats: PartitionQueueStats
        get() = PartitionQueueStats(
                lastNormalCommitMessageNumber,
                lastCommitMessageNumber,
                lastForceCommitRequestMessageNumber,
                lastConsumedMessageNumber,
                isPaused,
                isRevoked
        )

    /**
     * Add message to queue
     */
    fun push(messages: List<Message>) {
        if (!isFistMessageInit) {
            val firstMessage = messages[0]
            val previousMessageNumber = firstMessage.number - 1

            //fill state based on first consumed message number
            //last commit was exactly for [previousMessageNumber]
            lastNormalCommitMessageNumber = previousMessageNumber
            lastCommitMessageNumber = previousMessageNumber
            lastForceCommitRequestMessageNumber = previousMessageNumber
            lastConsumedMessageNumber = previousMessageNumber
            isFistMessageInit = true
        }

        queue.addAll(messages)

        //notify about new available data
        notifyOwner()
    }

    /**
     * Drain queue no longer [timeout] in time [unit].
     *
     * If no message available, or time exceeded, null will be returned.
     *
     * This method is not thread safe!
     */
    fun pop(): AssociatedMessage? {
        if (isRevoked) {
            if (Application.DEBUG) {
                //this is normal while unsubscribe to see this in logs
                logger.debug("Attempt to read revoked $associatedPartition for consumer $consumer")
            }
            return null
        }

        if (lastConsumedMessageNumber - lastNormalCommitMessageNumber >= consumeFactor) {
            return null
        }

        val message = queue.poll()
                ?: return null

        lastConsumedMessageNumber = message.number

        return AssociatedMessage(message, consumer, this)
    }

    /**
     * Try to commit completed messages.
     * We can commit only continuously. Thats mean
     * that we look in mask max sequence of set bit from the start
     * and commit only this, unless next bit are turns (i.e. task with
     * corresponding number are complete).
     *
     * If there is no state change since last call, commit
     * will not be performed.
     *
     * For unsubscribe processing [listener] can be specified for
     * listen next commit compleated events in form of message number.
     *
     * Method return an actual message number that is ready to commit.
     */
    fun commitPending(listener: Subject<Long>? = null): Long {
        if (listener != null) {
            commitListeners.add(listener)
        }

        // All consumed messages already commited
        if (lastConsumedMessageNumber == lastCommitMessageNumber) {
            onCommit(lastConsumedMessageNumber)

            return lastConsumedMessageNumber
        }

        commitLock.lock()

        val sequenceLength = getMaskSequenceLength()
        val actualMessageNumber = lastNormalCommitMessageNumber + sequenceLength

        // Nothing change since last call
        if (actualMessageNumber <= lastCommitMessageNumber) {
            commitLock.unlock()
            onCommit(actualMessageNumber)

            return actualMessageNumber
        }

        // Some force commit request already in action
        if (actualMessageNumber == lastForceCommitRequestMessageNumber) {
            commitLock.unlock()

            return actualMessageNumber
        }

        lastForceCommitRequestMessageNumber = actualMessageNumber
        doCommit(actualMessageNumber, CommitMode.FORCE)

        commitLock.unlock()

        return actualMessageNumber
    }

    /**
     * Mark message with given [messageNumber] as completed.
     *
     * If whole batch of [consumeFactor] messages since last normal commit are
     * completed, method do actual commit to the native queue.
     *
     * This method is lock-free thread safe, because tasks can be submitted by
     * multiple workers.
     */
    fun commit(messageNumber: Long): Boolean {
        if (Application.DEBUG) {
            logger.debug("Commit request for $associatedPartition of $consumer with offset: $messageNumber. ($stats)")
        }

        val expectedLastCommitMessageNumber = lastNormalCommitMessageNumber

        markBit(messageNumber)

        if (!isMaskFilled()) {
            return true
        }

        // in worst case two threads can commit same message twise, this is
        // a small price to pay for lock-free
        if (expectedLastCommitMessageNumber == lastNormalCommitMessageNumber) {
            doCommit(expectedLastCommitMessageNumber + consumeFactor)
        }
        return true
    }

    /**
     * Mark corresponding to [messageNumber] bit in mask.
     *
     * Lock-free atomic method.
     */
    private fun markBit(messageNumber: Long) {
        do {
            val difference = messageNumber - lastNormalCommitMessageNumber - 1
            val index = (difference % consumeFactor).toInt()

            val wordIndex = index / longWordSize
            val wordBitIndex = index % longWordSize

            val currentValue = commitedMask[wordIndex]
            val bitValue = currentValue.ushr(longWordSize - wordBitIndex - 1).and(1)

            //some desync detected
            if (difference > consumeFactor || bitValue == 1L) {
                throw RuntimeException("Cannot commit message $messageNumber before commit last $consumeFactor messages.")
            }

            val newValue = currentValue.or(1L.shl(longWordSize - wordBitIndex - 1))

        } while (!commitedMask.compareAndSet(wordIndex, currentValue, newValue))
    }

    /**
     * Count set bits from the start of mask until
     * false bit not found.
     */
    private fun getMaskSequenceLength(): Int {
        for (index in 0 until consumeFactor) {
            val wordIndex = index / longWordSize
            val wordBitIndex = index % longWordSize

            val bitValue = commitedMask[wordIndex].ushr(longWordSize - wordBitIndex - 1).and(1)
            if (bitValue != 1L) {
                return index
            }
        }

        return consumeFactor
    }

    /**
     * Mark all bits false
     */
    private fun clearMask() {
        for (index in 0..awaitedReferenceLength) {
            commitedMask[index] = 0
        }
    }

    /**
     * Check if mask are fully filled by comparing
     * mask with reference value
     */
    private fun isMaskFilled(): Boolean {
        for (index in 0..awaitedReferenceLength) {
            if (commitedMask[index] != referenceMaskForCommit[index]) {
                return false
            }
        }

        return true
    }

    private fun doCommit(messageNumber: Long, mode: CommitMode = CommitMode.NORMAL) {
        logger.info("Do commit for $associatedPartition of ${consumer.toTinyString()} with message number: $messageNumber. (mode=$mode)")

        group.doCommit(this, messageNumber, mode)
    }

    fun onBeforeCommit(messageNumber: Long, mode: CommitMode) {}

    /**
     * After commit was done by native queue
     */
    fun onAfterCommit(messageNumber: Long, mode: CommitMode) {
        commitLock.lock()

        if (mode == CommitMode.NORMAL) {
            clearMask()
            lastNormalCommitMessageNumber = messageNumber

            // notify about queue unlock
            notifyOwner()
        }

        lastCommitTimeMs = currentTimeMillis()
        lastCommitMessageNumber = messageNumber
        commitLock.unlock()

        onCommit(messageNumber)
    }

    /**
     * When commit failed, queue no longer active and must be revoked
     */
    fun onCommitFailed(messageNumber: Long, mode: CommitMode, throwable: Throwable) {
        revoke()

        onCommit(-1)
    }

    /**
     * Notify registered listeners
     */
    private fun onCommit(messageNumber: Long) {
        //optimize for iterator creating over nothing
        if (commitListeners.isNotEmpty()) {
            for (listener in commitListeners) {
                listener.onNext(messageNumber)
            }
        }
    }

    /**
     * Notify owner about state changes.
     */
    private fun notifyOwner() {
        owner?.onAction(this)
    }

    /**
     * Mark queue as paused
     */
    fun pause() {
        isPaused = true
    }

    /**
     * Unmark queue as paused
     */
    fun resume() {
        isPaused = false
    }

    /**
     * Revoke queue.
     * Since this, queue no longer be able to emit any messages.
     */
    fun revoke() {
        isRevoked = true

        //notify about revoking
        notifyOwner()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(this::class.java)
    }
}