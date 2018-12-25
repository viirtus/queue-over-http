# Queue-Over-Http

This project allows you to use Kafka as a Pub-Sub system over HTTP. 

## Applicable cases

**You already have a large REST infrastructure and you wanna maximize code utilization for pub-sub just in one minute.**

This project allows you to consume any messages from existing Kafka brokers over HTTP with almost zero configuration. 
Messages are passed directly to your endpoint in a HTTP-request! 

**You want to simplify consumers balancing.**  

This project allows you to translate balancing tasks from your backend to existing L4/L7 balancers in front of consumers. Messages are stateless and can easily 
be consumed by different services behind balancer. Latency from Queue-Over-Http in between of broker and consumers are extremely low! 

**You planning to use any web hooks (Slack, Jira, etc...)**

Queue-Over-Http allows you to use web hooks of any service without manually dispatching messages. Just write message to Kafka topic and 
Queue-Over-Http dispatch these messages for your services without you.

**You can't use native clients**

If you can't modify server backend (but can deploy new HTTP endpoint) or existing clients are not applicable, feel free to use Queue-Over-Http!

## Inapplicable cases

**Extra-low latency**

Despite of quite effective work of Queue-Over-Http, HTTP protocol are overweighted for some tasks and can lead to some additional work while transferring and processing.

**Exactly-once delivery semantic**

Current consume architecture are not suitable for implementing exactly-once delivery semantic because of lack of support from consumer side for implementing two-phase commit.
Current delivery semantic are **At-least-once** for each consumer.

**Very-high-reliability**

Queue-Over-Http are quite reliable, but not intended to use in critical systems. There is no replication yet for such reliability.

## Let's try!

1. First of all, make sure you have already running Kafka broker. Let's suppose, `bootstrap.servers` are equals to `localhost:9092`.
 Make sure your Kafka instance are ready for connections from Queue-Over-Http as a listener.
 
2. Download `docker-compose.yml` to some dir, a `qoh`, for example.
3. Let's navigate to `qoh` and write our configuration `application.yml` file:
```yaml
spring:
  profiles: default

logging:
  level:
    com:
      viirrtus:
        queueOverHttp: DEBUG

app:
  persistence:
    file:
      storageDirectory: "persist"
  brokers:
    - name: "Our Test Kafka Instance"
      origin: "kafka"
      config:
        bootstrap.servers: "localhost:9092"
```
We specify one broker named "Our Test Kafka Instance". Also, we specify directory for storing our subscribers - `persist`. 
Debug options are useful for first date.

Since this configuration are processed by Spring, you can place here any Spring-supported options.

4. Run service with `docker-compose up`. By default, HTTP part or Queue-Over-Http are start at 8080 port.  
5. Let's run some HTTP endpoint on `http://localhost:80/consume` for handling POST requests. It will be better if you log incoming headers and body to console.
6. Now, we can register our first consumer with next HTTP request:
```http request
POST localhost:8080/broker/subscription
Content-Type: application/json

{
  "id": "first-consumer",
  "group": {
    "id": "consumer-group"
  },
  "broker": "Our Test Kafka Instance",
  "topics": [
    {
      "name": "test",
      "config": {
        "concurrencyFactor": 50,
        "autoCommitPeriodMs": 1000
      }
    }
  ],
  "subscriptionMethod": {
    "type": "http",
    "delayOnErrorMs": 1000,
    "uri": "http://localhost:80/consume",
    "additionalHeaders": {
      "My-Auth-Token": "secret"
    }
  }
}
```
After doing request, check `persist`. You must find almost same payload in `first-consumer_consumer-group` file. This is done by persistence.

By default, message will be dispatched as a POST request. You can configure HTTP method using `subscriptionMethod.method` field.

Also, feel free to pass any additional data (such as auth) in each request using `subscriptionMethod.additionalHeaders`. 

7. Write some messages to `test` Kafka broker topic using some producer.
8. Watch for endpoint output and enjoy! 

## Building

You can build Queue-Over-Http from sources with `gradlew bootArtifacts` command. Built jar will be in `artifacts`.

### Running JAR

As a regular JAR, `java -jar -Dspring.profiles.active=default`. Config file `application.yml` must be in the same dir where jar. You can specify `--debug` flag to enable debug output (make sure `logging.level.com.viirrtus.queueOverHttp: DEBUG` are set).

## Usage 

Assume you run Queue-Over-Http on `127.0.0.1:8080`. 

### Consumer

Consumer are represented by [this](src/main/kotlin/com/viirrtus/queueOverHttp/dto) interfaces.

For now, only supported value for `Consumer.subscriptionMethod.type` is `http`.

### Subscription

Subscribe consumer to specified broker. Since this consumer are registered. 

```http request
POST 127.0.0.1:8080/broker/subscription
Content-Type: application/json

{
  "id": "myServiceIntanceName",
  "group": {
    "id": "myServiceName"
  },
  "broker": "Kafka",
  "topics": [
    {
      "name": "test",
      "config": {
        "concurrencyFactor": 50,
        "autoCommitPeriodMs": 1000
      }
    }
  ],
  "subscriptionMethod": {
    "type": "http",
    "delayOnErrorMs": 1,
    "uri": "http://your-consumer-here.org/"
  }
}
```

### Unsubscribe 

Same as subscribe, except for endpoint. Because 99% subscriptions done automatically by code, this method required full consumer.

Since this, consumer will never receive any message from specified broker.

```http request
POST 127.0.0.1:8080/broker/unsubscribe
Content-Type: application/json

{
  "id": "myServiceIntanceName",
  "group": {
    "id": "myServiceName"
  },
  "broker": "Kafka",
  "topics": [
    {
      "name": "test",
      "config": {
        "concurrencyFactor": 50,
        "autoCommitPeriodMs": 1000
      }
    }
  ],
  "subscriptionMethod": {
    "type": "http",
    "delayOnErrorMs": 1,
    "uri": "http://your-consumer-here.org/"
  }
}
```

### List 

You can list all registered in Queue-Over-Http consumers with:

```http request
GET 127.0.0.1:8080/broker/subscription
```

### Updating 

Updating consumers inflight are not supported yet. For updating topics for consumer or subscription method config you must unsubscribe first and subscribe again with new values.

## HTTP-message format

Each message dispatched over HTTP contains next standard headers:
- `QOH-Broker` - `string` - broker name from which message came
- `QOH-Consumer` - `string` - consumer identification in form of "${consumer.id}_${consumer.group.id}".
- `QOH-Topic` - `string` - topic name from witch message came.
- `QOH-Partition` - `string` - partition of topic, if applicable.
- `QOH-Message-Key` - `string` - partition message key, if applicable.
- `QOH-Message-Number` - `long` - message number from broker.
- `User-Agent` - `string` - always like `Queue-Over-Http`

In depends on selected HTTP-method (`Consumer.subscriptionMethod.method`), message can be dispatched in request body (for POST, PUT, PATCH, DELETE) requests, or as a query string parameter.
 Parameter name is define by `Consumer.subscriptionMethod.queryParamKey` which is `message` by default. 

Any additional headers can be specified in `Consumer.subscriptionMethod.additionalHeaders`. These headers are included in each HTTP-request for consumer.
 For example, you can specify `Content-Type: application/json` or any others.

## Configuration

All application configuration is done by `application.yml` file.

### app.persistence

Persistence is useful for keep subscriptions between service starts, or, for recovery in case of error. 

Consumer are stored in persistence on registration and delete from on unsubscribe.

Currently available two options:
- `app.persistence.file` - keep subscription in for of JSON-serialized files. Directory for holding are configured by `app.persistence.file.storageDirectory`.
- no persistence. If no options specified, subscriptions are not stored in external. 

### app.brokers

List of brokers to work with. Consumer can subscribe only for one specified here broker by it's name.

* `app.brokers[].name` - `string` - local broker name. This name is used by consumers on registration.
* `app.brokers[].origin` - `string` - broker system name. Available values - `kafka`. Other brokers not available yet.
* `app.brokers[].origin` - `<object>` - any broker-specific key-value configuration.
 [Kafka available options](https://github.com/apache/kafka/blob/2.0/clients/src/main/java/org/apache/kafka/clients/consumer/ConsumerConfig.java).
 For Kafka hardcoded values for key and value serializers to `LongDeserializer` and `StringDeserializer`. 

