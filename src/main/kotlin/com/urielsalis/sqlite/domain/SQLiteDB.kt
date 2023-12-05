package com.urielsalis.sqlite.domain

data class SQLiteDB(val header: SQLiteHeader)

data class SQLiteHeader(
    // 2 bytes
    val pageSize: Int,
    val fileFormatWriteVersion: FileFormatVersion,
    val fileFormatReadVersion: FileFormatVersion,
    val bytesReservedAtEndOfEachPage: Byte,
    val maxEmbeddedPayloadFraction: Byte,
    val minEmbeddedPayloadFraction: Byte,
    val leafPayloadFraction: Byte,
    val fileChangeCounter: Int,
    val databaseSizeInPages: Int,
    val firstFreelistTrunkPage: Int,
    val totalFreelistPages: Int,
    val schemaCookie: Int,
    val schemaFormatNumber: SchemaFormat,
    val defaultPageCacheSize: Int,
    val largestBTreePageNumber: Int,
    val databaseTextEncoding: TextEncoding,
    val userVersion: Int,
    val incrementalVacuumMode: Int,
    val applicationID: Int,
    // 20 bytes
    val reserved: ByteArray,
    val versionValidFor: Int,
    val sqliteVersionNumber: Int,
) {
    enum class FileFormatVersion(val value: Byte) {
        LEGACY(1),
        WAL(2),
        ;

        companion object {
            fun fromByte(byte: Byte): FileFormatVersion =
                when (byte) {
                    1.toByte() -> LEGACY
                    2.toByte() -> WAL
                    else -> throw IllegalArgumentException("Invalid FileFormatVersion byte: $byte")
                }
        }
    }

    @Suppress("MagicNumber")
    enum class TextEncoding(val value: Byte) {
        UTF8(1),
        UTF16LE(2),
        UTF16BE(3),
        ;

        companion object {
            fun fromByte(byte: Byte): TextEncoding =
                when (byte) {
                    0.toByte() -> UTF8 // Used in some tests where the header is not valid
                    1.toByte() -> UTF8
                    2.toByte() -> UTF16LE
                    3.toByte() -> UTF16BE
                    else -> throw IllegalArgumentException("Invalid TextEncoding byte: $byte")
                }
        }
    }

    @Suppress("MagicNumber")
    @JvmInline
    value class SchemaFormat(val value: Byte) {
        companion object {
            private const val MIN_VERSION = 0
            private const val MAX_VERSION = 4

            fun fromByte(value: Byte): SchemaFormat {
                if (value in MIN_VERSION..MAX_VERSION) {
                    return SchemaFormat((value))
                }
                throw IllegalArgumentException("Invalid SchemaFormat byte: $value")
            }
        }
    }
}
