package com.urielsalis.sqlite.domain

sealed class SQLQuery(open val table: String, open val conditions: Map<String, String>)

data class CountSQLQuery(override val table: String, override val conditions: Map<String, String>) :
    SQLQuery(table, conditions)

data class SelectSQLQuery(
    override val table: String,
    override val conditions: Map<String, String>,
    val columns: List<String>,
) : SQLQuery(table, conditions)
