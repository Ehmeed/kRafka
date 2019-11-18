package org.ehmeed.kRafka.r

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndTimestamp
import org.apache.kafka.common.TopicPartition
import java.time.Instant
import java.util.*
import kotlin.math.max

typealias StringConsumer = KafkaConsumer<String, String>

fun newConsumer(properties: Properties): StringConsumer = StringConsumer(properties)

fun read(
    consumer: StringConsumer,
    topic: String,
    type: String,
    from: String,
    to: String,
    timeout: Long,
    maxMessages: Long
): DataFrame = when(type) {
    "datetime" -> readByDate(consumer, topic, from, to, timeout, maxMessages)
    "offsets" -> readByOffsets(consumer, topic, from, to, timeout, maxMessages)
    else -> throw IllegalArgumentException("Only ${listOf("datetime", "offsets")} types are suppored ($type supplied)")
}

fun readByDate(
    consumer: StringConsumer,
    topic: String,
    from: String?,
    to: String?,
    timeout: Long,
    maxMessages: Long
): DataFrame {
    val timeStart = System.currentTimeMillis()
    val timestampFrom = Instant.parse(from).toEpochMilli()
    val timestampTo = Instant.parse(to).toEpochMilli()
    consumer.subscribe(listOf(topic))

    val assignment = consumer.assignment()
    val offsetsFrom = consumer.offsetsForTimes(assignment.associateWith { timestampFrom })
    val offsetsTo = consumer.offsetsForTimes(assignment.associateWith { timestampTo })

    offsetsFrom.forEach {
        consumer.seek(it.key, it.value.offset())
    }
    val dataFrame: DataFrame = DataFrame()
    while (!readOffests(consumer, offsetsTo) && !isTimeout(timeStart, timeout) && dataFrame.size < maxMessages) {

    }

    return dataFrame
}

private fun isTimeout(timeStart: Long, timeout: Long) = System.currentTimeMillis() - timeStart >= timeout

private fun readOffests(consumer: StringConsumer, offsetsTo: Map<TopicPartition, OffsetAndTimestamp>): Boolean {

}

fun readByOffsets(
    consumer: StringConsumer,
    topic: String,
    from: String?,
    to: String?,
    timeout: Long,
    maxMessages: Long
): DataFrame {
    TODO("☺")
}
