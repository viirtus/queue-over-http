package com.viirrtus.queueOverHttp.dto

import javax.validation.constraints.NotEmpty

/**
 * Consumer group, i.e. subscribed service.
 */
data class Group(
        @NotEmpty
        val id: String,

        /**
         * Help parameter, not used anywhere but list info.
         */
        val name: String = "NO_NAME"
)