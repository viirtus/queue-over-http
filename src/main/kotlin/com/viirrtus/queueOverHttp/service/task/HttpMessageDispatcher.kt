package com.viirrtus.queueOverHttp.service.task

import com.viirrtus.queueOverHttp.dto.HttpSubscriptionMethod
import com.viirrtus.queueOverHttp.queue.AssociatedMessage
import org.apache.http.HttpHeaders
import org.apache.http.HttpResponse
import org.apache.http.client.methods.*
import org.apache.http.client.utils.URIBuilder
import org.apache.http.concurrent.FutureCallback
import org.apache.http.entity.StringEntity
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient
import org.apache.http.impl.nio.client.HttpAsyncClients
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor
import org.apache.http.impl.nio.reactor.IOReactorConfig
import org.slf4j.LoggerFactory
import org.springframework.http.HttpMethod
import java.io.Closeable
import java.net.URI
import java.nio.charset.Charset
import java.util.concurrent.CompletableFuture
import kotlin.reflect.full.createInstance

/**
 * Dispatch tasks over HTTP using Apache Async Client.
 */
class HttpMessageDispatcher : Closeable {
    private val client: CloseableHttpAsyncClient

    init {
        //TODO externalize
        val ioReactorConfig = IOReactorConfig.custom()
                .setIoThreadCount(Runtime.getRuntime().availableProcessors() - 1)
                .setConnectTimeout(30000)
                .setSoTimeout(30000)
                .setSoReuseAddress(true)
                .setTcpNoDelay(true)
                .build()

        val poolingManager = PoolingNHttpClientConnectionManager(DefaultConnectingIOReactor(ioReactorConfig))

        client = HttpAsyncClients.custom()
                .setDefaultIOReactorConfig(ioReactorConfig)
                .setConnectionManager(poolingManager)
                .disableCookieManagement()
                .build()

        client.start()
    }

    override fun close() {
        client.close()
    }

    /**
     * Dispatch [message] by requesting to [uri].
     * Response will be wrapped in CompletableFuture.
     */
    fun dispatch(uri: String, message: AssociatedMessage): CompletableFuture<HttpResponse> {
        val request = constructRequest(uri, message)

        val future = CompletableFuture<HttpResponse>()
        client.execute(request, object : FutureCallback<HttpResponse> {
            override fun cancelled() {
                future.completeExceptionally(InterruptedException("Http request was unexpectedly canceled."))
            }

            override fun completed(result: HttpResponse?) {
                future.complete(result)
            }

            override fun failed(ex: Exception?) {
                future.completeExceptionally(ex)
            }

        })

        return future
    }

    private fun constructRequest(uri: String, message: AssociatedMessage): HttpRequestBase {
        val subscriptionMethod = message.consumer.subscriptionMethod as HttpSubscriptionMethod

        //all classes has the same constructor
        val requestClass = methodToClass[subscriptionMethod.method]!!
        val request = requestClass.createInstance()

        val charset = Charset.forName(subscriptionMethod.charset)
        if (request is HttpEntityEnclosingRequestBase) { //has body part
            val entity = StringEntity(message.message.body, charset)
            request.entity = entity
            request.uri = URI.create(uri)
        } else {
            val builder = URIBuilder(uri)
            builder.addParameter(subscriptionMethod.queryParamKey, String(message.message.body.toByteArray(), charset))
            request.uri = builder.build()
        }

        subscriptionMethod.additionalHeaders.forEach { k, v ->
            request.addHeader(k, v)
        }

        addStandardHeaders(request, message)

        return request
    }

    private fun addStandardHeaders(request: HttpRequestBase, message: AssociatedMessage) {
        request.addHeader(HttpHeaders.USER_AGENT, USER_AGENT)
        request.addHeader(CONSUMER_HEADER, message.consumer.toTinyString())
        request.addHeader(TOPIC_HEADER, message.queue.associatedPartition.topic)
        request.addHeader(PARTITION_HEADER, message.queue.associatedPartition.partition)
        request.addHeader(BROKER_HEADER, message.consumer.broker)
        request.addHeader(KEY_HEADER, message.message.key)
        request.addHeader(NUMBER_HEADER, message.message.number.toString())
    }

    companion object {
        private val methodToClass = mapOf(
                //has body part
                HttpMethod.POST to HttpPost::class,
                HttpMethod.PUT to HttpPut::class,
                HttpMethod.PATCH to HttpPatch::class,
                HttpMethod.DELETE to HttpDelete::class,

                //without body part
                HttpMethod.GET to HttpGet::class,
                HttpMethod.HEAD to HttpHead::class,
                HttpMethod.OPTIONS to HttpOptions::class,
                HttpMethod.TRACE to HttpTrace::class
        )

        const val USER_AGENT = "Queue-Over-Http"
        const val CONSUMER_HEADER = "QOH-Consumer"
        const val TOPIC_HEADER = "QOH-Topic"
        const val PARTITION_HEADER = "QOH-Partition"
        const val BROKER_HEADER = "QOH-Broker"
        const val KEY_HEADER = "QOH-Message-Key"
        const val NUMBER_HEADER = "QOH-Message-Number"

        private val logger = LoggerFactory.getLogger(HttpMessageDispatcher::class.java)
    }
}