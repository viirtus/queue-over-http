package com.viirrtus.queueOverHttp.controller

import com.fasterxml.jackson.core.JsonProcessingException
import com.viirrtus.queueOverHttp.service.exception.ApiException
import org.slf4j.LoggerFactory
import org.springframework.core.NestedRuntimeException
import org.springframework.http.HttpStatus
import org.springframework.web.HttpRequestMethodNotSupportedException
import org.springframework.web.bind.MethodArgumentNotValidException
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.ResponseBody
import org.springframework.web.servlet.NoHandlerFoundException
import javax.servlet.http.HttpServletResponse

@ControllerAdvice()
class RestExceptionResolver {
    private val logger = LoggerFactory.getLogger(RestExceptionResolver::class.java)

    @ExceptionHandler()
    @ResponseBody
    fun handleException(exception: Exception, response: HttpServletResponse): Map<String, Any?> {
        var reason = "Unknown exception"
        var code = HttpStatus.INTERNAL_SERVER_ERROR
        var details = "none"

        val actualException: Throwable = if (exception is NestedRuntimeException) {
            exception.mostSpecificCause
        } else {
            exception
        }

        when (actualException) {
            is ApiException -> {
                reason = actualException.message!!
                code = actualException.status
            }
            is JsonProcessingException -> {
                reason = "JSON_EXCEPTION"
                code = HttpStatus.BAD_REQUEST
            }
            is HttpRequestMethodNotSupportedException -> {
                reason = "METHOD_NOT_SUPPORTED"
                code = HttpStatus.METHOD_NOT_ALLOWED
            }

            is NoHandlerFoundException -> {
                reason = "NOT_FOUND"
                code = HttpStatus.NOT_FOUND
            }

            is MethodArgumentNotValidException -> {
                reason = "ARGUMENT_NOT_VALID"
                code = HttpStatus.BAD_REQUEST
                details = actualException.message ?: details
            }
            else -> {
                logger.error("Unexpected exception", exception)
            }
        }

        response.status = code.value()
        return mapOf("success" to false, "reason" to reason, "details" to details)
    }
}
