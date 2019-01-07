package com.viirrtus.queueOverHttp.service.exception

import org.springframework.http.HttpStatus

open class ApiException(message: String?, val status: HttpStatus, cause: Throwable? = null) : Exception(message, cause)