package com.urielsalis.sqlite

fun main(args: Array<String>) {
    if (args.size < 2) {
        println("Missing <database path> and <command>")
        return
    }

    val databaseFilePath = args[0]
    val command = args[1]

    val db = parseDb(databaseFilePath)
    when (command) {
        ".dbinfo" -> {
            println("database page size: " + db.header.pageSize)
        }

        else -> println("Missing or invalid command passed: $command")
    }
}
