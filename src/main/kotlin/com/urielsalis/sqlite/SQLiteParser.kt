package com.urielsalis.sqlite

import com.urielsalis.sqlite.domain.SQLiteDB
import com.urielsalis.sqlite.domain.SQLiteHeader
import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteOrder

private const val HEADER_SIZE = 100

fun parseDb(databaseFilePath: String): SQLiteDB {
    val file = File(databaseFilePath).inputStream()
    val header = ByteBuffer.wrap(file.readNBytes(HEADER_SIZE)).order(ByteOrder.BIG_ENDIAN)
    return SQLiteDB(parseHeader(header))
}

@Suppress("MagicNumber")
fun parseHeader(headerBuf: ByteBuffer): SQLiteHeader {
    val magic = ByteArray(16)
    headerBuf.get(magic)
    require(String(magic) == "SQLite format 3\u0000")
    val pageSize =
        headerBuf.getShort().let {
            if (it == 1.toShort()) {
                65536u
            } else {
                it.toUInt()
            }
        }
    val fileFormatWriteVersion = SQLiteHeader.FileFormatVersion.fromByte(headerBuf.get())
    val fileFormatReadVersion = SQLiteHeader.FileFormatVersion.fromByte(headerBuf.get())
    val bytesReservedAtEndOfEachPage = headerBuf.get()
    val maxEmbeddedPayloadFraction = headerBuf.get().mustBe(64)
    val minEmbeddedPayloadFraction = headerBuf.get().mustBe(32)
    val leafPayloadFraction = headerBuf.get().mustBe(32)
    val fileChangeCounter = headerBuf.getInt()
    val databaseSizeInPages = headerBuf.getInt()
    val firstFreelistTrunkPage = headerBuf.getInt()
    val totalFreelistPages = headerBuf.getInt()
    val schemaCookie = headerBuf.getInt()
    val schemaFormatNumber = SQLiteHeader.SchemaFormat.fromByte(headerBuf.get())
    val defaultPageCacheSize = headerBuf.getInt()
    val largestBTreePageNumber = headerBuf.getInt()
    val databaseTextEncoding = SQLiteHeader.TextEncoding.fromByte(headerBuf.get())
    val userVersion = headerBuf.getInt()
    val incrementalVacuumMode = headerBuf.getInt()
    val applicationID = headerBuf.getInt()
    val reserved = ByteArray(20)
    headerBuf.get(reserved)
    val versionValidFor = headerBuf.getInt()
    val sqliteVersionNumber = headerBuf.getInt()
    return SQLiteHeader(
        pageSize.toInt(),
        fileFormatWriteVersion,
        fileFormatReadVersion,
        bytesReservedAtEndOfEachPage,
        maxEmbeddedPayloadFraction,
        minEmbeddedPayloadFraction,
        leafPayloadFraction,
        fileChangeCounter,
        databaseSizeInPages,
        firstFreelistTrunkPage,
        totalFreelistPages,
        schemaCookie,
        schemaFormatNumber,
        defaultPageCacheSize,
        largestBTreePageNumber,
        databaseTextEncoding,
        userVersion,
        incrementalVacuumMode,
        applicationID,
        reserved,
        versionValidFor,
        sqliteVersionNumber,
    )
}
