package com.viirrtus.queueOverHttp.dto

/**
 * Consumer group, i.e. subscribed service.
 */
data class Group(
        val id: String,

        /**
         * Help parameter, not used anywhere but list info.
         */
        val name: String = "NO_NAME"
)