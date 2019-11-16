package org.ehmeed.kRafka.r

class DataFrame(
    val keys: MutableList<String?> = mutableListOf(),
    val timestamps: MutableList<Long?> = mutableListOf(),
    val values: MutableList<String?> = mutableListOf()
) {
    fun append(key: String?, timestamp: Long?, value: String?) {
        keys.add(key)
        timestamps.add(timestamp)
        values.add(value)
    }

    fun append(keys: List<String?>, timestamps: List<Long?>, values: List<String?>) {
        this.keys.addAll(keys)
        this.timestamps.addAll(timestamps)
        this.values.addAll(values)
    }
}

data class TopicOffsetsListing(
    val firstMessageOffset: Long,
    val firstMessageTimestamp: String,
    val lastMessageOffset: Long,
    val lastMessageTimestamp: String
)
