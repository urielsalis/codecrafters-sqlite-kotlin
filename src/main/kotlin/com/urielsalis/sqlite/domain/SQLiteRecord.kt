package com.urielsalis.sqlite.domain

data class SQLiteRecord(val rowId: Long, val values: List<List<SQLiteValue>>)

sealed interface SQLiteValue {
    fun size() = 0
}

sealed interface NumberSQLiteValue : SQLiteValue {
    fun getNumber(): Number
}

data object NullSQLiteValue : SQLiteValue {
    override fun toString(): String = "null"
}

data class ByteSQLiteValue(val value: Byte) : NumberSQLiteValue {
    override fun size(): Int = 1

    override fun getNumber(): Number = value

    override fun toString(): String = value.toString()
}

data class ShortSQLiteValue(val value: Short) : NumberSQLiteValue {
    override fun size(): Int = 2

    override fun getNumber(): Number = value

    override fun toString(): String = value.toString()
}

@Suppress("MagicNumber")
data class ThreeByteSQLiteValue(val value: Int) : NumberSQLiteValue {
    override fun size(): Int = 3

    override fun getNumber(): Number = value

    override fun toString(): String = value.toString()
}

data class IntSQLiteValue(val value: Int) : NumberSQLiteValue {
    override fun size(): Int = 4

    override fun getNumber(): Number = value

    override fun toString(): String = value.toString()
}

@Suppress("MagicNumber")
data class FiveByteSQLiteValue(val value: Long) : NumberSQLiteValue {
    override fun size(): Int = 5

    override fun getNumber(): Number = value

    override fun toString(): String = value.toString()
}

data class LongSQLiteValue(val value: Long) : NumberSQLiteValue {
    override fun size(): Int = 8

    override fun getNumber(): Number = value

    override fun toString(): String = value.toString()
}

data class DoubleSQLiteValue(val value: Double) : NumberSQLiteValue {
    override fun size(): Int = 8

    override fun getNumber(): Number = value

    override fun toString(): String = value.toString()
}

data class LiteralSQLiteValue(val value: Int) : NumberSQLiteValue {
    override fun getNumber() = value

    override fun toString(): String =
        if (value != 0) {
            "true"
        } else {
            "false"
        }
}

data class BlobSQLiteValue(val size: Int, val value: ByteArray) : SQLiteValue {
    override fun size(): Int = size

    override fun toString(): String = value.joinToString("") { "%02x".format(it) }
}

data class StringSQLiteValue(val size: Int, val value: String) : SQLiteValue {
    override fun size(): Int = size

    override fun toString(): String = value
}
