package org.ehmeed.kRafka.r

data class DataFrame(
    val keys: List<String?> = listOf(),
    val timestamps: List<Long?> = listOf(),
    val values: List<String?> = listOf()
)

val DataFrame.size: Int
    get() = keys.size

fun DataFrame.append(keys: List<String?>, timestamps: List<Long?>, values: List<String?>) = DataFrame(
    this.keys + keys,
    this.timestamps + timestamps,
    this.values + values
)

fun DataFrame.append(key: String?, timestamp: Long?, value: String?) = DataFrame(
    this.keys + key,
    this.timestamps + timestamp,
    this.values + value
)

fun DataFrame.head(n: Int) = DataFrame(keys.take(n), timestamps.take(n), values.take(n))
