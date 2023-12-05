package com.urielsalis.sqlite

import com.urielsalis.sqlite.domain.CountSQLQuery
import com.urielsalis.sqlite.domain.SQLQuery

fun parseQuery(command: String): SQLQuery {
    val fromIndex = command.indexOf("FROM", 0, true)
    val whereIndex = command.indexOf("WHERE", 0, true)
    val (table, conditions) =
        if (whereIndex != -1) {
            val table = command.substring(fromIndex + 5, whereIndex).trim()
            val conditions =
                command.substring(whereIndex + 5).trim().split("AND").associate {
                    val split = it.trim().split("=")
                    split[0].trim() to split[1].trim()
                }
            table to conditions
        } else {
            command.substring(fromIndex + 5).trim() to emptyMap()
        }
    if (command.startsWith("SELECT COUNT", true)) {
        return CountSQLQuery(table, conditions)
    } else {
        TODO()
    }
}

fun parseColumnNames(value: String): List<String> {
    val clean = value.replace("\n", "").replace("\r", "").replace("\t", "").trim()
    require(clean.startsWith("CREATE TABLE", true))
    val columns = value.substringAfter("(").substringBeforeLast(")").split(",")
    return columns.map { it.trim().substringBefore(" ") }
}
