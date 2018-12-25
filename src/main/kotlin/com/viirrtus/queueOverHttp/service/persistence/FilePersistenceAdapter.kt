package com.viirrtus.queueOverHttp.service.persistence

import com.fasterxml.jackson.databind.ObjectMapper
import com.viirrtus.queueOverHttp.Application
import com.viirrtus.queueOverHttp.config.AppConfig
import com.viirrtus.queueOverHttp.config.FilePersistenceConfig
import com.viirrtus.queueOverHttp.dto.Consumer
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Lazy
import org.springframework.stereotype.Component
import java.io.File
import javax.annotation.PostConstruct

/**
 * Keep consumers in form of serialized with [objectMapper] files
 * in specified by [FilePersistenceConfig.storageDirectory] directory.
 */
@Lazy
@Component
class FilePersistenceAdapter(
        private val config: FilePersistenceConfig,
        private val objectMapper: ObjectMapper
) : PersistenceAdapterInterface {
    private val targetDirPath = config.storageDirectory + File.separator

    @PostConstruct
    fun checkDir() {
        val targetDir = File(targetDirPath)
        if (!targetDir.exists()) {
            val dirCreated = targetDir.mkdirs()

            if (!dirCreated) {
                throw RuntimeException("Cannot create directory: $targetDirPath. Check FS permissions.")
            }
        }

        if (!targetDir.canWrite()) {
            throw RuntimeException("Directory $targetDirPath are not writable. Check FS permissions.")
        }
    }

    /**
     * Write consumer as file.
     */
    override fun store(consumer: Consumer) {
        val target = getTargetFile(consumer)
        objectMapper.writeValue(target, consumer)
    }

    /**
     * Simply remove associated to consumer file.
     */
    override fun remove(consumer: Consumer) {
        val target = getTargetFile(consumer)

        if (target.exists()) {
            val deleteSuccess = target.delete()
            if (!deleteSuccess) {
                logger.warn("Unable to delete file ${target.absolutePath}.")
                target.deleteOnExit()
            }
        } else {
            if (Application.DEBUG) {
                logger.debug("Nothing to remove. No stored consumer found in ${target.absolutePath}.")
            }
        }
    }

    /**
     * Read all files in [targetDirPath] and deserialize each.
     */
    override fun restore(): List<Consumer> {
        val targetDir = File(targetDirPath)
        return targetDir.listFiles()
                .asSequence()
                .filter { it.isFile }
                .map { file ->
                    objectMapper.readValue(file, Consumer::class.java)
                }.toList()
    }

    private fun getTargetFile(consumer: Consumer): File = File(targetDirPath + consumer.toTinyString())

    companion object {
        private val logger = LoggerFactory.getLogger(this::class.java)
    }
}