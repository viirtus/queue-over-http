package com.viirrtus.queueOverHttp.service.exception

import org.springframework.http.HttpStatus

open class ApiException(message: String?, status: HttpStatus, cause: Throwable? = null) : Exception(message, cause)