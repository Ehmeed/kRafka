.onLoad <- function(libname, pkgname) {
  library(rJava)
  .jpackage(name = pkgname, jars = "*")
  .jinit()
}


.newConsumer <- function(
  bootstrapServers,
  groupId,
  keyDeserializer,
  valueDeserializer,
  configs
  ) {
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

  J("org.ehmeed.kRafka.r.ApiKt")$newConsumer(properties)
}

.read <- function(
  consumer,
  topic,
  type,
  from,
  to,
  timeout,
  maxMessages
) {
  df = J("org.ehmeed.kRafka.r.ApiKt")$read(
    consumer,
    .jnew("java/lang/String", topic),
    .jnew("java/lang/String", type),
    .jnew("java/lang/String", from),
    .jnew("java/lang/String", to),
    .jlong(timeout),
    .jlong(maxMessages)
  )
  .toDataFrame(df)
}


.close <- function(consumer, timeout = NULL) {
  if (is.null(timeout)) {
    consumer$close()
  } else {
    stopifnot(is.numeric(timeout))
    consumer$close(.toDuration(timeout))
  }
  consumer
}

kRafka.listTopics <- function(
  bootstrapServers,
  groupId = paste("kRafka_consumer_", paste(sample(letters, 12), collapse=""), sep = ""),
  keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer",
  valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer",
  consumerConfigs = list(),
  timeout = NULL
  ) {
  consumer = .newConsumer(bootstrapServers, groupId, keyDeserializer, valueDeserializer, consumerConfigs)
  if (is.null(timeout)) {
    topics = consumer$listTopics()
  } else {
    stopifnot(is.numeric(timeout))
    topics = consumer$listTopics(.toDuration(.timeout))
  }
  .close(consumer)
  sapply(as.list(topics$keySet()), function(topic) topic$toString())
}

kRafka.read <- function(
  bootstrapServers,
  topic,
  type = "datetime",
  from = "1970-01-01T00:00:00.00Z",
  to = "2037-01-01T00:00:00.00Z",
  timeout = .Machine$integer.max,
  maxMessages = .Machine$integer.max,
  groupId = paste("kRafka_consumer_", paste(sample(letters, 12), collapse=""), sep = ""),
  keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer",
  valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer",
  consumerConfigs = list()
  ) {

  consumer = .newConsumer(bootstrapServers, groupId, keyDeserializer, valueDeserializer, consumerConfigs)
  df = .read(consumer, topic, type, from, to, timeout, maxMessages)
  .close(consumer)
  df
}
