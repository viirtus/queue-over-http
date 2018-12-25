package com.viirrtus.queueOverHttp

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.core.env.ConfigurableEnvironment
import org.springframework.web.servlet.config.annotation.EnableWebMvc
import javax.annotation.PostConstruct


@SpringBootApplication
@EnableWebMvc
open class Application {
    companion object {
        var DEBUG = false
    }
}

fun main(args: Array<String>) {

    for (arg in args) {
        if (arg.toLowerCase() == "debug") {
            Application.DEBUG = true
        }
    }

    runApplication<Application>(*args)
}



