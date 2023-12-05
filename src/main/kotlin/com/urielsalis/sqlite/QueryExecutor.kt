package com.urielsalis.sqlite

import com.urielsalis.sqlite.domain.CountSQLQuery
import com.urielsalis.sqlite.domain.LongSQLiteValue
import com.urielsalis.sqlite.domain.NullSQLiteValue
import com.urielsalis.sqlite.domain.NumberSQLiteValue
import com.urielsalis.sqlite.domain.SQLQuery
import com.urielsalis.sqlite.domain.SQLiteDB
import com.urielsalis.sqlite.domain.SQLiteIndex
import com.urielsalis.sqlite.domain.SQLitePage
import com.urielsalis.sqlite.domain.SQLitePageHeader
import com.urielsalis.sqlite.domain.SQLiteRecord
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
    val indexes = db.schema.indexes.filter { it.tableName == query.table }
    if (allConditionsAreIndexed(query, indexes)) {
        return executeQueryWithIndex(db, query, table, indexes)
    }
    val rootPage = db.getPage(table.rootPage)
    return when (query) {
        is CountSQLQuery -> executeCountQuery(db, query, table, rootPage)
        is SelectSQLQuery -> executeSelect(db, query, table, rootPage)
    }
}

fun executeQueryWithIndex(
    db: SQLiteDB,
    query: SQLQuery,
    table: SQLiteTable,
    indexes: List<SQLiteIndex>,
): List<List<String>> {
    val indexesNeeded = indexes.filter { it.indexedColumn in query.conditions.keys }
    val rowIdsPerIndex =
        indexesNeeded.associate {
            it.indexedColumn to
                getRowsForIndex(
                    db, it, query.conditions[it.indexedColumn]!!,
                )
        }
    val rowIds = rowIdsPerIndex.values.reduce { acc, list -> acc.intersect(list.toSet()).toList() }
    val rootPage = db.getPage(table.rootPage)
    val cells = rowIds.mapNotNull { findCell(db, rootPage, it) }
    if (query is CountSQLQuery) {
        return listOf(listOf(cells.flatMap { it.values }.size.toString()))
    }
    if (query !is SelectSQLQuery) {
        error("Invalid query type")
    }
    return cells.map {
        table.columns.flatMapIndexed { index, column ->
            when (column) {
                !in query.columns -> emptyList()
                "id" -> listOf(it.rowId.toString())
                else -> it.values.map { it[index].toString() }
            }
        }
    }
}

fun findCell(
    db: SQLiteDB,
    rootPage: SQLitePage,
    rowId: Long,
): SQLiteRecord? {
    for (cell in rootPage.cells) {
        if (cell is SQLitePageHeader.LeafTableCell) {
            if (cell.rowId == rowId) {
                return parseRecord(db.header.databaseTextEncoding, cell)
            }
        }
        if (cell is SQLitePageHeader.InteriorTableCell && rowId <= cell.rowId) {
            val page = db.getPage(cell.leftChildPage)
            val result = findCell(db, page, rowId)
            if (result != null) {
                return result
            }
        }
    }
    if (!rootPage.header.pageType.hasRightMostPointer()) {
        return null
    }
    val page = db.getPage(rootPage.header.rightMostPointer)
    return findCell(db, page, rowId)
}

fun getRowsForIndex(
    db: SQLiteDB,
    index: SQLiteIndex,
    expectedValue: String,
): List<Long> {
    val rootPage = db.getPage(index.rootPage)
    val result = mutableListOf<Long>()
    rootPage.cells.forEachIndexCell(db, index, expectedValue) {
        result.add(it)
    }
    return result
}

fun allConditionsAreIndexed(
    query: SQLQuery,
    indexes: List<SQLiteIndex>,
) = query.conditions.isNotEmpty() &&
    query.conditions.keys.all { key ->
        indexes.any { it.indexedColumn == key }
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

private fun MutableList<SQLitePageHeader.Cell>.forEachIndexCell(
    db: SQLiteDB,
    index: SQLiteIndex,
    expectedValue: String,
    func: Consumer<Long>,
) {
    forEach { cell ->
        if (cell is SQLitePageHeader.CellWithPayload) {
            parseRecord(db.header.databaseTextEncoding, cell).values.forEach { row ->
                val key = row[0]
                val rowId = row[1]
                require(rowId is NumberSQLiteValue)
                if (key is StringSQLiteValue && key.value == expectedValue) {
                    func.accept(rowId.getNumber().toLong() and 0xFFFFFFFF)
                }
            }
        }
        if (cell is SQLitePageHeader.CellWithPageReference) {
            val page = db.getPage(cell.leftChildPage)
            page.cells.forEachIndexCell(db, index, expectedValue, func)
        }
    }
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
