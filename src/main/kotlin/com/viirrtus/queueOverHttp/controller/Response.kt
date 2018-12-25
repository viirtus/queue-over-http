package com.viirrtus.queueOverHttp.controller

data class Response<T>(
        val data: T,
        val success: Boolean = true
)