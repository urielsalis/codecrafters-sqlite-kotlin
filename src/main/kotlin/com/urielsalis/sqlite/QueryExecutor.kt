package com.urielsalis.sqlite

import com.urielsalis.sqlite.domain.CountSQLQuery
import com.urielsalis.sqlite.domain.IntSQLiteValue
import com.urielsalis.sqlite.domain.SQLQuery
import com.urielsalis.sqlite.domain.SQLiteDB
import com.urielsalis.sqlite.domain.SQLitePageHeader
import com.urielsalis.sqlite.domain.SQLiteRecord
import com.urielsalis.sqlite.domain.SQLiteTable
import com.urielsalis.sqlite.domain.SQLiteValue
import java.util.function.Consumer

fun executeQuery(
    db: SQLiteDB,
    query: SQLQuery,
): SQLiteRecord {
    if (query !is CountSQLQuery) {
        TODO()
    }
    val table = db.schema.tables.find { it.tableName == query.table } ?: error("Table not found")
    val rootPage = db.getPage(table.rootPage)
    var counter = 0
    rootPage.cells.forEachCell(db, table) {
        if (matchesConditions(it, query.conditions)) {
            counter++
        }
    }
    return SQLiteRecord(-1, listOf(listOf(IntSQLiteValue(counter))))
}

fun matchesConditions(
    map: Map<String, SQLiteValue>,
    conditions: Map<String, String>,
): Boolean {
    conditions.forEach { (key, value) ->
        if (map[key]?.equals(value) == false) {
            return false
        }
    }
    return true
}

private fun MutableList<SQLitePageHeader.Cell>.forEachCell(
    db: SQLiteDB,
    table: SQLiteTable,
    func: Consumer<Map<String, SQLiteValue>>,
) {
    forEach {
        val record = parseRecord(db.header.databaseTextEncoding, it)
        record.values.forEach { row ->
            val map = mutableMapOf<String, SQLiteValue>()
            table.columns.forEachIndexed { index, column ->
                map[column] = row[index]
            }
            func.accept(map)
        }
    }
}
