package com.viirrtus.queueOverHttp.service

import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Service
import kotlin.reflect.KClass

@Service
class ComponentLocator(
        private val context: ApplicationContext
) {

    fun <T : Any> locate(clazz: KClass<T>): T {
        return context.getBean(clazz.java)
    }

}