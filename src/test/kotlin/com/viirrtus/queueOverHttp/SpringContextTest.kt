package com.viirrtus.queueOverHttp

import com.viirrtus.queueOverHttp.config.FilePersistenceConfig
import com.viirrtus.queueOverHttp.controller.TestController
import org.junit.After
import org.junit.Before
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner
import java.io.File


@RunWith(SpringRunner::class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@ActiveProfiles("test")
abstract class SpringContextTest {

    init {
        Application.DEBUG = true
    }

    @Autowired
    private lateinit var config: FilePersistenceConfig

    @Before
    fun prepare() {
        TestController.receivedMessages.clear()
    }

    @After
    fun clear() {
        val targetDir = config.storageDirectory
        val dir = File(targetDir)
        if (dir.exists()) {
            dir.deleteRecursively()
        }
    }

    @TestConfiguration
    open class Config {

    }
}