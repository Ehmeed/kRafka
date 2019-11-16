package org.ehmeed.kRafka.r

import org.ehmeed.kRafka.kafka.KRafkaConsumer
import java.util.*

fun newConsumer(properties: Properties): KRafkaConsumer = KRafkaConsumer.newConsumer(properties)

fun read(
    consumer: KRafkaConsumer,
    topic: String,
    type: String = "datetime",
    from: String? = "1970-01-01 00:00:00",
    to: String? = "2030-01-01 00:00:00",
    timeout: Long = Long.MAX_VALUE,
    maxMessages: Long = Long.MAX_VALUE
): DataFrame = TODO()

fun listTopics(): List<String> = TODO()

fun listTopicOffsets(topic: String): TopicOffsetsListing = TODO()

fun close(consumer: KRafkaConsumer): Unit = TODO()
