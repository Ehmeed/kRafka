
kRafka.newConsumer <- function(
  bootstrapServers,
  groupId = paste("kRafka_consumer_", paste(sample(letters, 12), collapse=""), sep = ""),
  keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer",
  valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer",
  configs = list()) {
  stopifnot(is.character(bootstrapServers))
  stopifnot(is.character(groupId))
  stopifnot(is.character(keyDeserializer))
  stopifnot(is.character(valueDeserializer))
  stopifnot(is.list(configs))
  properties <- .listToProps(configs)
  .putToProps(properties, "bootstrap.servers", bootstrapServers)
  .putToProps(properties, "group.id", groupId)
  .putToProps(properties, "key.deserializer", keyDeserializer)
  .putToProps(properties, "value.deserializer", valueDeserializer)

  .jnew("org/apache/kafka/clients/consumer/KafkaConsumer", properties)
}

kRafka.poll <- function(consumer, timeout) {
  consumer$poll(.toDuration(timeout))
}

kRafka.listTopics <- function(consumer, timeout = NULL) {
  if (is.null(timeout)) {
    topics = consumer$listTopics()
  } else {
    stopifnot(is.numeric(timeout))
    topics = consumer$listTopics(.toDuration(timeout))
  }
  sapply(as.list(topics$keySet()), function(topic) topic$toString())
}

kRafka.close <- function(consumer, timeout = NULL) {
  if (is.null(timeout)) {
    consumer$close()
  } else {
    stopifnot(is.numeric(timeout))
    consumer$close(.toDuration(timeout))
  }
}

#close
#subscribe

