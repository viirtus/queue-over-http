package com.viirrtus.queueOverHttp.service

import com.viirrtus.queueOverHttp.SpringContextTest
import com.viirrtus.queueOverHttp.originConsumer
import com.viirrtus.queueOverHttp.service.persistence.FilePersistenceAdapter
import org.junit.Assert
import org.junit.Test
import org.springframework.test.annotation.DirtiesContext
import javax.annotation.Resource


@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
class TestFilePersistenceAdapter : SpringContextTest() {
    @Resource(name = "filePersistenceAdapter")
    private lateinit var adapter: FilePersistenceAdapter

    @Test
    fun `test if consumers stored fine`() {
        val consumers = (0..10).map { originConsumer.copy(id = "$it") }

        for (consumer in consumers) {
            adapter.store(consumer)
        }

        val storedConsumers = adapter.restore()
        Assert.assertTrue(storedConsumers.isNotEmpty())

        for (storedConsumer in storedConsumers) {
            val originConsumer = consumers.find { it.same(storedConsumer) }
            Assert.assertNotNull(originConsumer)
        }

        for (consumer in consumers) {
            adapter.remove(consumer)
        }

        val emptyStoredConsumers = adapter.restore()
        Assert.assertTrue(emptyStoredConsumers.isEmpty())
    }

}