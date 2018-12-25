package com.viirrtus.queueOverHttp.controller

import com.viirrtus.queueOverHttp.config.AppConfig
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.ResponseBody
import org.springframework.web.bind.annotation.RestController
import javax.servlet.http.HttpServletRequest

/**
 * Used only for inplace testing
 */
@RestController
class TestController(
        val config: AppConfig
) {

    @PostMapping("/test")
    @ResponseBody
    fun test(@RequestBody(required = false) body: String?, request: HttpServletRequest): ResponseEntity<String> {

//        if (Math.random() > 0.1) {
//            return ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR)
//        }
        return ResponseEntity.ok("OK")
    }
}