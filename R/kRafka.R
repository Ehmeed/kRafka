# Hello, world!
#
# This is an example function named 'hello'
# which prints 'Hello, world!'.
#
# You can learn more about package authoring with RStudio at:
#
#   http://r-pkgs.had.co.nz/
#
# Some useful keyboard shortcuts for package authoring:
#
#   Install Package:           'Ctrl + Shift + B'
#   Check Package:             'Ctrl + Shift + E'
#   Test Package:              'Ctrl + Shift + T'

.onLoad <- function(libname, pkgname) {
  library(rJava)
  .jpackage(name = pkgname, jars = "*")
  .jinit()
}


hello <- function() {
  print("Hello, world!")
}

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



.listToProps <- function(list) {
  properties <- .jnew("java/util/Properties")
  for (key in names(list)) {
    .putToProps(properties, key, list[[key]])
  }
  properties
}

.putToProps <- function(properties, key, value) {
  stopifnot(is.character(key))
  stopifnot(is.character(value))
  key = .jnew("java/lang/String", key)
  value = .jcast(.jnew("java/lang/String", value), "java/lang/Object")
  properties$put(key, value)
}





