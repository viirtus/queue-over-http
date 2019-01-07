package com.viirrtus.queueOverHttp.dto

import javax.validation.constraints.NotBlank

/**
 * For low-level partition handling.
 */
data class Partition(
        @field:NotBlank
        val number: String = "0"
)