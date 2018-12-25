package com.viirrtus.queueOverHttp.service.exception

import org.springframework.http.HttpStatus

class IllegalConsumerConfigException(message: String?) : ApiException(message, HttpStatus.BAD_REQUEST)