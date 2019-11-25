package org.ehmeed.kRafka.r

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndTimestamp
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import java.time.Instant
import java.util.*

typealias StringConsumer = KafkaConsumer<String, String>

public fun newConsumer(properties: Properties): StringConsumer = StringConsumer(properties)

public fun read(
    consumer: StringConsumer,
    topic: String,
    type: String,
    from: String,
    to: String,
    timeout: Long,
    maxMessages: Long
): DataFrame = when (type) {
    "datetime" -> readByDate(consumer, topic, from, to, timeout, maxMessages)
    "offsets" -> readByOffsets(consumer, topic, from, to, timeout, maxMessages)
    else -> throw IllegalArgumentException("Only ${listOf("datetime", "offsets")} types are suppored ($type supplied)")
}

public fun readByDate(
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
    while (consumer.assignment().isEmpty() && !isTimeout(timeStart, timeout)) {
        consumer.poll(Duration.ofMillis(1000))
    }

    val assignment = consumer.assignment()
    val offsetsFrom = consumer.offsetsForTimes(assignment.associateWith { timestampFrom })
    val offsetsTo: Map<TopicPartition, OffsetAndTimestamp?> = consumer.offsetsForTimes(assignment.associateWith { timestampTo })

    offsetsFrom.forEach {
        consumer.seek(it.key, it.value.offset())
    }
    val dataFrame = DataFrame()
    while (!exceededOffsets(consumer, offsetsTo) && !isTimeout(timeStart, timeout) && dataFrame.size < maxMessages) {
        consumer.poll(Duration.ofMillis(timeout - (System.currentTimeMillis() - timeStart)))
            .forEach { dataFrame.append(it.key(), it.timestamp(), it.value()) }
    }

    return dataFrame
}

private fun isTimeout(timeStart: Long, timeout: Long) = System.currentTimeMillis() - timeStart >= timeout

private fun exceededOffsets(consumer: StringConsumer, offsetsTo: Map<TopicPartition, OffsetAndTimestamp?>): Boolean {
    val currentOffsets = consumer.assignment().map { it.partition() to consumer.position(it) }
    val maxOffsets = offsetsTo.toList().filter { it.second != null }.map { it.first.partition() to it.second!!.offset() }.toMap()
    return currentOffsets.all { (partition, offset) -> offset >= maxOffsets.getOrDefault(partition, Long.MAX_VALUE) }
}

private fun readByOffsets(
    consumer: StringConsumer,
    topic: String,
    from: String?,
    to: String?,
    timeout: Long,
    maxMessages: Long
): DataFrame {
    TODO("☺")
}

fun main() {
    val p = Properties().apply {
        put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        put("group.id", "asdasdasdasduiahdkjansd")
        put("bootstrap.servers", "sdp-develop1.lundegaard.net:9093")
    }
    val consumer = newConsumer(p)
    val frame = readByDate(
        consumer,
        "afs_cofisun_flags",
        "2000-01-01T00:00:00.00Z",
        "2030-01-01T00:00:00.00Z",
        10000000000000L,
        1000000000000000L
    )

    println(frame.size)
}
