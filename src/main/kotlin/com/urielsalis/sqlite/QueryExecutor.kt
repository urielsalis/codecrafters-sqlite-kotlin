package com.urielsalis.sqlite

import com.urielsalis.sqlite.domain.CountSQLQuery
import com.urielsalis.sqlite.domain.LongSQLiteValue
import com.urielsalis.sqlite.domain.NullSQLiteValue
import com.urielsalis.sqlite.domain.SQLQuery
import com.urielsalis.sqlite.domain.SQLiteDB
import com.urielsalis.sqlite.domain.SQLitePage
import com.urielsalis.sqlite.domain.SQLitePageHeader
import com.urielsalis.sqlite.domain.SQLiteTable
import com.urielsalis.sqlite.domain.SQLiteValue
import com.urielsalis.sqlite.domain.SelectSQLQuery
import com.urielsalis.sqlite.domain.StringSQLiteValue
import java.util.function.Consumer

fun executeQuery(
    db: SQLiteDB,
    query: SQLQuery,
): List<List<String>> {
    val table = db.schema.tables.find { it.tableName == query.table } ?: error("Table not found")
    val rootPage = db.getPage(table.rootPage)
    return when (query) {
        is CountSQLQuery -> executeCountQuery(db, query, table, rootPage)
        is SelectSQLQuery -> executeSelect(db, query, table, rootPage)
    }
}

fun executeSelect(
    db: SQLiteDB,
    query: SelectSQLQuery,
    table: SQLiteTable,
    rootPage: SQLitePage,
): List<List<String>> {
    val result = mutableListOf<List<String>>()
    rootPage.cells.forEachCell(db, table) {
        if (matchesConditions(it, query.conditions)) {
            val row = mutableListOf<String>()
            query.columns.forEach { column ->
                row.add(it[column]?.toString() ?: "null")
            }
            result.add(row)
        }
    }
    return result
}

fun executeCountQuery(
    db: SQLiteDB,
    query: CountSQLQuery,
    table: SQLiteTable,
    rootPage: SQLitePage,
): List<List<String>> {
    var counter = 0
    rootPage.cells.forEachCell(db, table) {
        if (matchesConditions(it, query.conditions)) {
            counter++
        }
    }
    return listOf(listOf(counter.toString()))
}

fun matchesConditions(
    map: Map<String, SQLiteValue>,
    conditions: Map<String, String>,
): Boolean {
    conditions.forEach { (key, expected) ->
        val value = map[key] ?: return@matchesConditions false
        if (value !is StringSQLiteValue) {
            return@matchesConditions false
        }
        return@matchesConditions value.value == expected
    }
    return true
}

private fun MutableList<SQLitePageHeader.Cell>.forEachCell(
    db: SQLiteDB,
    table: SQLiteTable,
    func: Consumer<Map<String, SQLiteValue>>,
) {
    forEach { cell ->
        if (cell is SQLitePageHeader.CellWithPayload) {
            parseRecord(db.header.databaseTextEncoding, cell).values.forEach { row ->
                val map = mutableMapOf<String, SQLiteValue>()
                table.columns.forEachIndexed { index, column ->
                    map[column] = row[index]
                }
                if (map["id"] == null || map["id"] is NullSQLiteValue) {
                    if (cell is SQLitePageHeader.CellWithRowId) {
                        map["id"] = LongSQLiteValue(cell.rowId)
                    }
                }
                func.accept(map)
            }
        }
        if (cell is SQLitePageHeader.CellWithPageReference) {
            val page = db.getPage(cell.leftChildPage)
            page.cells.forEachCell(db, table, func)
        }
    }
}
