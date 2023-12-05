package com.urielsalis.sqlite

fun <T> T.mustBe(value: T): T = value.also { require(this == value) { "Invalid value: $this" } }
