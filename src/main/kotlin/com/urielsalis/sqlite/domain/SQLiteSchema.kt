package com.urielsalis.sqlite.domain

class SQLiteSchema(
    val size: Int,
    val tables: List<SQLiteTable>,
    val indexes: List<SQLiteIndex>,
    val triggers: List<SQLiteTrigger>,
    val views: List<SQLiteView>,
)

data class SQLiteTable(
    val internalName: String,
    val tableName: String,
    val rootPage: Int,
    val sql: String,
    val columns: List<String>,
)

data class SQLiteIndex(
    val indexName: String,
    val tableName: String,
    val rootPage: Int,
    val sql: String,
    val indexedColumn: String,
)

data class SQLiteTrigger(
    val internalName: String,
    val tableName: String,
    val rootPage: Int,
    val sql: String,
)

data class SQLiteView(
    val internalName: String,
    val tableName: String,
    val rootPage: Int,
    val sql: String,
)
