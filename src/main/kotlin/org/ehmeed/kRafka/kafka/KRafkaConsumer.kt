package org.ehmeed.kRafka.kafka

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.*


class KRafkaConsumer private constructor(private val consumer: KafkaConsumer<String, String>) :
    Consumer<String, String> by consumer {

    companion object Factory {
        fun newConsumer(props: Properties) = KRafkaConsumer(KafkaConsumer(props))
    }
}
