package com.urielsalis.sqlite.domain

import com.urielsalis.sqlite.getNBytes
import com.urielsalis.sqlite.getVarInt
import java.nio.ByteBuffer

data class SQLitePage(val header: SQLitePageHeader, val cells: MutableList<SQLitePageHeader.Cell>)

data class SQLitePageHeader(
    val pageType: PageType<out Cell>,
    val firstFreeblockOffset: Short,
    val numberOfCells: Short,
    // 2 bytes
    val cellContentAreaOffset: Int,
    val fragmentedFreeBytes: Byte,
    val rightMostPointer: Int,
) {
    sealed interface PageType<T : Cell> {
        fun hasRightMostPointer() = false

        fun parseCell(buffer: ByteBuffer): T = error("Invalid PageType: $this")
        companion object {
            fun fromByte(value: Byte) =
                when (value) {
                    0x02.toByte() -> InteriorIndexPageType
                    0x05.toByte() -> InteriorTablePageType
                    0x0A.toByte() -> LeafIndexPageType
                    0x0D.toByte() -> LeafTablePageType
                    else -> error("Invalid PageType byte: $value")
                }
        }
    }

    sealed class Cell

    data class InteriorIndexCell(val leftChildPage: Int, val payload: ByteArray) : Cell()

    data class InteriorTableCell(val leftChildPage: Int, val rowId: Long) : Cell()

    data class LeafIndexCell(val payload: ByteArray) : Cell()

    data class LeafTableCell(val rowId: Long, val payload: ByteArray) : Cell()

    data object InteriorIndexPageType : PageType<InteriorIndexCell> {
        override fun hasRightMostPointer() = true

        override fun parseCell(buffer: ByteBuffer): InteriorIndexCell {
            val leftChildPage = buffer.getInt()
            val payloadSize = buffer.getVarInt()
            val payload = buffer.getNBytes(payloadSize.toInt())
            return InteriorIndexCell(leftChildPage, payload)
        }
    }
    data object InteriorTablePageType : PageType<InteriorTableCell> {
        override fun hasRightMostPointer() = true

        override fun parseCell(buffer: ByteBuffer): InteriorTableCell {
            val leftChildPage = buffer.getInt()
            val rowId = buffer.getVarInt()
            return InteriorTableCell(leftChildPage, rowId)
        }
    }
    data object LeafIndexPageType : PageType<LeafIndexCell> {
        override fun parseCell(buffer: ByteBuffer): LeafIndexCell {
            val payloadSize = buffer.getVarInt()
            val payload = buffer.getNBytes(payloadSize.toInt())
            return LeafIndexCell(payload)
        }
    }
    data object LeafTablePageType : PageType<LeafTableCell> {
        override fun parseCell(buffer: ByteBuffer): LeafTableCell {
            val payloadSize = buffer.getVarInt()
            val rowId = buffer.getVarInt()
            val payload = buffer.getNBytes(payloadSize.toInt())
            return LeafTableCell(rowId, payload)
        }
    }
}
