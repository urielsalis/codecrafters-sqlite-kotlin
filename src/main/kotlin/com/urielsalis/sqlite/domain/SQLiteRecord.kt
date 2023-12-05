package com.urielsalis.sqlite.domain

data class SQLiteRecord(val values: List<List<SQLiteValue>>)

sealed interface SQLiteValue {
    fun size() = 0
}

sealed interface NumberSQLiteValue : SQLiteValue {
    fun getNumber(): Number
}

data object NullSQLiteValue : SQLiteValue

data class ByteSQLiteValue(val value: Byte) : NumberSQLiteValue {
    override fun size(): Int = 1

    override fun getNumber(): Number = value
}

data class ShortSQLiteValue(val value: Short) : NumberSQLiteValue {
    override fun size(): Int = 2

    override fun getNumber(): Number = value
}

@Suppress("MagicNumber")
data class ThreeByteSQLiteValue(val value: Int) : NumberSQLiteValue {
    constructor(byteArray: ByteArray) : this(
        byteArray[0].toInt() shl 16
            or (byteArray[1].toInt() shl 8)
            or byteArray[2].toInt(),
    )

    override fun size(): Int = 3

    override fun getNumber(): Number = value
}

data class IntSQLiteValue(val value: Int) : NumberSQLiteValue {
    override fun size(): Int = 4

    override fun getNumber(): Number = value
}

@Suppress("MagicNumber")
data class FiveByteSQLiteValue(val value: Long) : NumberSQLiteValue {
    constructor(byteArray: ByteArray) : this(
        byteArray[0].toLong() shl 32
            or (byteArray[1].toLong() shl 24)
            or (byteArray[2].toLong() shl 16)
            or (byteArray[3].toLong() shl 8)
            or byteArray[4].toLong(),
    )

    override fun size(): Int = 5

    override fun getNumber(): Number = value
}

data class LongSQLiteValue(val value: Long) : NumberSQLiteValue {
    override fun size(): Int = 8

    override fun getNumber(): Number = value
}

data class DoubleSQLiteValue(val value: Double) : NumberSQLiteValue {
    override fun size(): Int = 8

    override fun getNumber(): Number = value
}

data class LiteralSQLiteValue(val value: Int) : NumberSQLiteValue {
    override fun getNumber() = value
}

data class BlobSQLiteValue(val size: Int, val value: ByteArray) : SQLiteValue {
    override fun size(): Int = size
}

data class StringSQLiteValue(val size: Int, val value: String) : SQLiteValue {
    override fun size(): Int = size
}
