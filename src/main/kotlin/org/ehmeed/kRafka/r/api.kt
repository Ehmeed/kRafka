package org.ehmeed.kRafka.r

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException
import java.lang.Long.max
import java.time.Duration
import java.time.Instant
import java.util.*
import kotlin.NoSuchElementException

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
    "datetime" -> readByDate(consumer, topic, from, to, timeout, maxMessages.toInt())
    "offsets" -> readByOffsets(consumer, topic, from, to, timeout, maxMessages.toInt())
    else -> throw IllegalArgumentException("Only ${listOf("datetime", "offsets")} types are supported ($type supplied)")
}

public fun readByDate(
    consumer: StringConsumer,
    topic: String,
    from: String,
    to: String,
    timeout: Long,
    maxMessages: Int
): DataFrame {
    val timestampStart = System.currentTimeMillis()
    val timestampFrom = Instant.parse(from).toEpochMilli()
    val timestampTo = Instant.parse(to).toEpochMilli()
    val timeLeft = { Duration.ofMillis(max(1L, timeout - (System.currentTimeMillis() - timestampStart))) }
    val isTimeLeft =
        { if (timeLeft().toMillis() <= 0) throw TimeoutException("Exceeded timeout while reading messages") else true }

    val topicPartitions = getTopicPartitions(consumer, timeLeft, topic)

    val (offsetsFrom, offsetsTo) = getOffsetsRangeForTime(
        topicPartitions, timestampFrom, consumer, timeLeft, timestampTo
    )
    consumer.assign(offsetsFrom.keys)
    consumer.seekOffsets(offsetsFrom)
    var dataFrame = DataFrame()

    while (dataFrame.size < maxMessages && isTimeLeft() && consumer.assignment().isNotEmpty()) {
        val singlePollTimeout = Duration.ofMillis(1000)
        val (correctRecords, incorrectRecords) = consumer.poll(singlePollTimeout).records(topic)
            .partition { it.offset() < offsetsTo.offsetForPartition(it.partition()) }
        correctRecords.forEach { dataFrame = dataFrame.append(it.key(), it.timestamp(), it.value()) }
        if (incorrectRecords.isNotEmpty()) {
            val finishedPartitions = incorrectRecords.map { it.partition() }
            val updatedAssignment = consumer.assignment().filter { it.partition() in finishedPartitions }
            consumer.assign(updatedAssignment)
        } else if(correctRecords.isEmpty()) {
            break
        }
    }

    return dataFrame.head(maxMessages)
}

private fun Map<TopicPartition, Long>.offsetForPartition(partition: Int): Long {
    return this.filter { it.key.partition() == partition }.values.toList().first()
}

private fun StringConsumer.seekOffsets(
    offsetsFrom: Map<TopicPartition, Long>
) {
    offsetsFrom.forEach {
        seek(it.key, it.value)
    }
}

private fun getOffsetsRangeForTime(
    topicPartitions: List<TopicPartition>,
    timestampFrom: Long,
    consumer: StringConsumer,
    timeLeft: () -> Duration,
    timestampTo: Long
): Pair<Map<TopicPartition, Long>, Map<TopicPartition, Long>> {
    val offsetsFrom = topicPartitions.associateWith { timestampFrom }
        .let { consumer.offsetsForTimes(it, timeLeft()) }
        .filter { it.value != null }.mapValues { it.value.offset() }

    return offsetsFrom to getValidOffsetsTo(topicPartitions, timestampTo, consumer, timeLeft)
}

private fun getValidOffsetsTo(topicPartitions: List<TopicPartition>,
                              timestampTo: Long,
                              consumer: StringConsumer,
                              timeLeft: () -> Duration): Map<TopicPartition, Long> {
    val offsetsTo = topicPartitions.associateWith { timestampTo }
        .let { consumer.offsetsForTimes(it, timeLeft()) }

    return if (null in offsetsTo.values) {
          (offsetsTo.filter { it.value != null }.mapValues { it.value.offset() } + consumer.endOffsets(topicPartitions, timeLeft()))
    } else {
        offsetsTo.mapValues { it.value.offset() }
    }
}

private fun getTopicPartitions(
    consumer: StringConsumer,
    timeLeft: () -> Duration,
    topic: String
): List<TopicPartition> {
    val topicInfo = consumer.listTopics(timeLeft())[topic]
        ?: throw NoSuchElementException("Topic $topic is not present in the cluster")
    return topicInfo.map { TopicPartition(it.topic(), it.partition()) }
}

private fun readByOffsets(
    consumer: StringConsumer,
    topic: String,
    from: String,
    to: String,
    timeout: Long,
    maxMessages: Int
): DataFrame {
    TODO("Offsets based reading not supported yet")
}
