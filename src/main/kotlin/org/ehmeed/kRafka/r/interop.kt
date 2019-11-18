package org.ehmeed.kRafka.r

class DataFrame(
    val keys: MutableList<String?> = mutableListOf(),
    val timestamps: MutableList<Long?> = mutableListOf(),
    val values: MutableList<String?> = mutableListOf()
) {
    val size: Int
        get() = keys.size

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
