package com.viirrtus.queueOverHttp.service.exception

import org.springframework.http.HttpStatus

class ConsumerUnavailableException(message: String?) : ApiException(message, HttpStatus.INTERNAL_SERVER_ERROR) {
}