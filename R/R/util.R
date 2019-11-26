
.listToProps <- function(list) {
  properties <- .jnew("java/util/Properties")
  for (key in names(list)) {
    .putToProps(properties, key, list[[key]])
  }
  properties
}

.stringVectorToCollection <- function(vec) {
  arrayList <- .jnew("java/util/ArrayList")
  for (value in vec) {
    arrayList$add(.jnew("java/lang/String", value))
  }
  .jcast(arrayList, "java/util/Collection")
}


.putToProps <- function(properties, key, value) {
  stopifnot(is.character(key))
  stopifnot(is.character(value))
  key = .jnew("java/lang/String", key)
  value = .jcast(.jnew("java/lang/String", value), "java/lang/Object")
  properties$put(key, value)
}


.toDuration <- function(time) {
  stopifnot(is.numeric(time))
  J("java.time.Duration")$ofMillis(.jlong(time))
}

.toDataFrame <- function (df) {
  timestamps <- sapply (as.list(df$getTimestamps()), function(item) as.numeric(item$toString()))
  keys <- sapply (as.list(df$getKeys()), function(item) item$toString())
  values <- sapply (as.list(df$getValues()), function(item) item$toString())
  data.frame(key = keys, timestamp = timestamps, value = values)
}
