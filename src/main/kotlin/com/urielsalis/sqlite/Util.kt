package com.urielsalis.sqlite

import java.nio.ByteBuffer

fun <T> T.mustBe(value: T): T = value.also { require(this == value) { "Invalid value: $this" } }

@Suppress("MagicNumber")
fun ByteBuffer.getVarInt(): Long {
    var result: Long = 0
    for (ignored in 0..7) {
        val current = get()
        result = (result shl 7) + (current.toInt() and 0x7F)
        if (current.toInt() and 0x80 == 0) {
            return result
        }
    }
    val last = get()
    result = (result shl 8) + last
    return result
}

fun ByteBuffer.getNBytes(n: Int): ByteArray {
    val result = ByteArray(n)
    get(result)
    return result
}

fun ByteBuffer.getNByteNumber(n: Int): Long {
    require(n in 1..8)
    var result = 0L
    for (i in 0 until n) {
        result = (result shl 8) + (get().toInt() and 0xff)
    }
    return result
}
