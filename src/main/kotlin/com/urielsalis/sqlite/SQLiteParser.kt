package com.urielsalis.sqlite

import com.urielsalis.sqlite.domain.BlobSQLiteValue
import com.urielsalis.sqlite.domain.ByteSQLiteValue
import com.urielsalis.sqlite.domain.DoubleSQLiteValue
import com.urielsalis.sqlite.domain.FiveByteSQLiteValue
import com.urielsalis.sqlite.domain.IntSQLiteValue
import com.urielsalis.sqlite.domain.LiteralSQLiteValue
import com.urielsalis.sqlite.domain.LongSQLiteValue
import com.urielsalis.sqlite.domain.NullSQLiteValue
import com.urielsalis.sqlite.domain.NumberSQLiteValue
import com.urielsalis.sqlite.domain.SQLiteDB
import com.urielsalis.sqlite.domain.SQLiteHeader
import com.urielsalis.sqlite.domain.SQLiteIndex
import com.urielsalis.sqlite.domain.SQLitePage
import com.urielsalis.sqlite.domain.SQLitePageHeader
import com.urielsalis.sqlite.domain.SQLiteRecord
import com.urielsalis.sqlite.domain.SQLiteSchema
import com.urielsalis.sqlite.domain.SQLiteTable
import com.urielsalis.sqlite.domain.SQLiteTrigger
import com.urielsalis.sqlite.domain.SQLiteValue
import com.urielsalis.sqlite.domain.SQLiteView
import com.urielsalis.sqlite.domain.ShortSQLiteValue
import com.urielsalis.sqlite.domain.StringSQLiteValue
import com.urielsalis.sqlite.domain.ThreeByteSQLiteValue
import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteOrder

private const val HEADER_SIZE = 100

fun parseDb(databaseFilePath: String): SQLiteDB {
    val file = File(databaseFilePath).inputStream()
    val header =
        parseHeader(ByteBuffer.wrap(file.readNBytes(HEADER_SIZE)).order(ByteOrder.BIG_ENDIAN))
    val schemaBuffer = ByteBuffer.allocate(header.pageSize)
    file.channel.position(0).read(schemaBuffer)
    val schema =
        parseSchema(header.databaseTextEncoding, schemaBuffer.rewind().order(ByteOrder.BIG_ENDIAN))
    val pages = mutableListOf<SQLitePage>()
    for (i in 1 until header.databaseSizeInPages) {
        val buffer = ByteBuffer.allocate(header.pageSize)
        file.channel.position(i * header.pageSize.toLong()).read(buffer)
        pages.add(parsePage(buffer.rewind().order(ByteOrder.BIG_ENDIAN)))
    }

    return SQLiteDB(header, schema, pages)
}

@Suppress("LongMethod")
private fun parseSchema(
    textEncoding: SQLiteHeader.TextEncoding,
    buffer: ByteBuffer,
): SQLiteSchema {
    val pageBuffer = buffer.position(HEADER_SIZE)
    val page = parsePage(pageBuffer)
    val tables = mutableListOf<SQLiteTable>()
    val indexes = mutableListOf<SQLiteIndex>()
    val triggers = mutableListOf<SQLiteTrigger>()
    val views = mutableListOf<SQLiteView>()
    val records =
        page.cells.mapNotNull {
            if (it is SQLitePageHeader.CellWithPayload) {
                parseRecord(textEncoding, it)
            } else {
                null
            }
        }
    for (record in records) {
        for (row in record.values) {
            val type = row[0]
            val name = row[1]
            val tblName = row[2]
            val rootPage = row[3]
            val sql = row[4]
            require(type is StringSQLiteValue)
            require(name is StringSQLiteValue)
            require(tblName is StringSQLiteValue)
            require(rootPage is NumberSQLiteValue)
            require(sql is StringSQLiteValue)
            when (type.value) {
                "table" ->
                    tables.add(
                        SQLiteTable(
                            name.value,
                            tblName.value,
                            rootPage.getNumber().toInt(),
                            sql.value,
                            parseColumnNames(sql.value),
                        ),
                    )

                "index" ->
                    indexes.add(
                        SQLiteIndex(
                            name.value,
                            tblName.value,
                            rootPage.getNumber().toInt(),
                            sql.value,
                        ),
                    )

                "trigger" ->
                    triggers.add(
                        SQLiteTrigger(
                            name.value,
                            tblName.value,
                            rootPage.getNumber().toInt(),
                            sql.value,
                        ),
                    )

                "view" ->
                    views.add(
                        SQLiteView(
                            name.value,
                            tblName.value,
                            rootPage.getNumber().toInt(),
                            sql.value,
                        ),
                    )
            }
        }
    }
    return SQLiteSchema(
        pageBuffer.limit() - page.header.cellContentAreaOffset - HEADER_SIZE,
        tables,
        indexes,
        triggers,
        views,
    )
}

fun parseRecord(
    textEncoding: SQLiteHeader.TextEncoding,
    cell: SQLitePageHeader.CellWithPayload,
): SQLiteRecord =
    SQLiteRecord(
        if (cell is SQLitePageHeader.CellWithRowId) cell.rowId else -1,
        parseRecordContent(textEncoding, cell.payload),
    )

private fun parseRecordContent(
    textEncoding: SQLiteHeader.TextEncoding,
    payload: ByteArray,
): MutableList<MutableList<SQLiteValue>> {
    val buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN)
    val headerSize = buffer.getVarInt()
    val columns = parseRecordColumns(buffer, headerSize)
    val rows = mutableListOf<MutableList<SQLiteValue>>()
    while (buffer.hasRemaining()) {
        rows.add(parseRecordColumnContent(columns, buffer, textEncoding))
    }
    return rows
}

private fun parseRecordColumnContent(
    columns: MutableList<SQLiteValue>,
    buffer: ByteBuffer,
    textEncoding: SQLiteHeader.TextEncoding,
): MutableList<SQLiteValue> {
    val row = mutableListOf<SQLiteValue>()
    columns.forEach {
        row.add(
            when (it) {
                is NullSQLiteValue -> NullSQLiteValue
                is ByteSQLiteValue -> ByteSQLiteValue(buffer.get())
                is ShortSQLiteValue -> ShortSQLiteValue(buffer.getShort())
                is ThreeByteSQLiteValue -> ThreeByteSQLiteValue(buffer.getNBytes(3))
                is IntSQLiteValue -> IntSQLiteValue(buffer.getInt())
                is FiveByteSQLiteValue -> FiveByteSQLiteValue(buffer.getNBytes(5))
                is LongSQLiteValue -> LongSQLiteValue(buffer.getLong())
                is DoubleSQLiteValue -> DoubleSQLiteValue(buffer.getDouble())
                is LiteralSQLiteValue -> it.copy()
                is BlobSQLiteValue -> BlobSQLiteValue(it.size, buffer.getNBytes(it.size))
                is StringSQLiteValue ->
                    StringSQLiteValue(
                        it.size,
                        buffer.getNBytes(it.size).toString(textEncoding.charset),
                    )
            },
        )
    }
    return row
}

@Suppress("CyclomaticComplexMethod", "MagicNumber")
private fun parseRecordColumns(
    buffer: ByteBuffer,
    headerSize: Long,
): MutableList<SQLiteValue> {
    val columns = mutableListOf<SQLiteValue>()
    while (buffer.position() < headerSize) {
        val type = buffer.getVarInt()
        columns.add(
            when (type) {
                0L -> NullSQLiteValue
                1L -> ByteSQLiteValue(0)
                2L -> ShortSQLiteValue(0)
                3L -> ThreeByteSQLiteValue(0)
                4L -> IntSQLiteValue(0)
                5L -> FiveByteSQLiteValue(0)
                6L -> LongSQLiteValue(0)
                7L -> DoubleSQLiteValue(0.0)
                8L -> LiteralSQLiteValue(0)
                9L -> LiteralSQLiteValue(1)
                10L, 11L -> error("Reserved type: $type")
                else -> {
                    if (type % 2 == 0L) {
                        val size = ((type - 12) / 2).toInt()
                        BlobSQLiteValue(size, ByteArray(size))
                    } else {
                        val size = ((type - 13) / 2).toInt()
                        StringSQLiteValue(size, "")
                    }
                }
            },
        )
    }
    return columns
}

private fun parsePage(buffer: ByteBuffer): SQLitePage {
    try {
        val header = parsePageHeader(buffer)
        val cellOffsets = mutableListOf<Short>()
        for (i in 0 until header.numberOfCells) {
            cellOffsets.add(buffer.getShort())
        }
        val cells = mutableListOf<SQLitePageHeader.Cell>()
        cellOffsets.sorted().forEach {
            buffer.position(it.toInt())
            cells.add(header.pageType.parseCell(buffer))
        }
        return SQLitePage(header, cells)
    } catch (e: IllegalStateException) {
        return SQLitePage(
            SQLitePageHeader(SQLitePageHeader.InvalidPageType, 0, 0, 0, 0, 0),
            mutableListOf(),
        )
    }
}

fun parsePageHeader(buffer: ByteBuffer): SQLitePageHeader {
    val type = SQLitePageHeader.PageType.fromByte(buffer.get())
    val firstFreeblockOffset = buffer.getShort()
    val numberOfCells = buffer.getShort()
    val contentStartArea =
        buffer.getShort().let {
            if (it == 0.toShort()) {
                65536
            } else {
                it.toUShort().toInt()
            }
        }
    val fragmentedFreeBytes = buffer.get()
    val rightMostPointer =
        if (type.hasRightMostPointer()) {
            buffer.getInt()
        } else {
            0
        }
    return SQLitePageHeader(
        type,
        firstFreeblockOffset,
        numberOfCells,
        contentStartArea,
        fragmentedFreeBytes,
        rightMostPointer,
    )
}

@Suppress("MagicNumber")
private fun parseHeader(headerBuf: ByteBuffer): SQLiteHeader {
    val magic = headerBuf.getNBytes(16)
    require(String(magic) == "SQLite format 3\u0000")
    val pageSize =
        headerBuf.getShort().let {
            if (it == 1.toShort()) {
                65536
            } else {
                it.toUShort().toInt()
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
    val reserved = headerBuf.getNBytes(20)
    val versionValidFor = headerBuf.getInt()
    val sqliteVersionNumber = headerBuf.getInt()
    return SQLiteHeader(
        pageSize,
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
