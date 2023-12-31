package com.urielsalis.sqlite

import com.urielsalis.sqlite.domain.CountSQLQuery
import com.urielsalis.sqlite.domain.SQLQuery
import com.urielsalis.sqlite.domain.SelectSQLQuery

fun parseQuery(command: String): SQLQuery {
    val fromIndex = command.indexOf("FROM", 0, true)
    val whereIndex = command.indexOf("WHERE", 0, true)
    val (table, conditions) =
        if (whereIndex != -1) {
            val table = command.substring(fromIndex + 5, whereIndex).trim()
            val conditions =
                command.substring(whereIndex + 5).trim().split("AND").associate {
                    val split = it.trim().split("=")
                    split[0].trim() to split[1].replace("'", "").trim()
                }
            table to conditions
        } else {
            command.substring(fromIndex + 5).trim() to emptyMap()
        }
    if (command.startsWith("SELECT COUNT", true)) {
        return CountSQLQuery(table, conditions)
    } else {
        val columns = command.substring(6, fromIndex).split(",").map { it.trim() }
        return SelectSQLQuery(table, conditions, columns)
    }
}

fun parseColumnNames(value: String): List<String> {
    val clean = value.cleanSqlQuery()
    require(clean.startsWith("CREATE TABLE", true))
    val columns = clean.substringAfter("(").substringBeforeLast(")").split(",")
    return columns.map { it.trim().substringBefore(" ") }
}

fun parseIndex(value: String): String {
    val clean = value.cleanSqlQuery()
    require(clean.startsWith("CREATE INDEX", true))
    return clean.substringAfter("(").substringBeforeLast(")")
}

private fun String.cleanSqlQuery() = replace("\n", "").replace("\r", "").replace("\t", "").trim()
