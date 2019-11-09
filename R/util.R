
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


.toDuration <- function(time) {
  stopifnot(is.numeric(time))
  J("java.time.Duration")$ofMillis(.jlong(time))
}
