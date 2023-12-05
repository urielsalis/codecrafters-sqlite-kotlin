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
            println("write format: " + db.header.fileFormatWriteVersion)
            println("read format: " + db.header.fileFormatReadVersion)
            println("reserved bytes: " + db.header.bytesReservedAtEndOfEachPage)
            println("file change counter: " + db.header.fileChangeCounter)
            println("database page count: " + db.header.databaseSizeInPages)
            println("freelist page count: " + db.header.totalFreelistPages)
            println("schema cookie: " + db.header.schemaCookie)
            println("schema format: " + db.header.schemaFormatNumber)
            println("default cache size: " + db.header.defaultPageCacheSize)
            println("autovacuum top root: " + db.header.largestBTreePageNumber)
            println("incremental vacuum: " + db.header.incrementalVacuumMode)
            println("text encoding: " + db.header.databaseTextEncoding)
            println("user version: " + db.header.userVersion)
            println("application id: " + db.header.applicationID)
            println("software version: " + db.header.sqliteVersionNumber)
            println("number of tables: " + db.schema.tables.size)
            println("number of indexes: " + db.schema.indexes.size)
            println("number of triggers: " + db.schema.triggers.size)
            println("number of views: " + db.schema.views.size)
            println("schema size: " + db.schema.size)
            println("data version " + db.header.versionValidFor)
        }

        else -> println("Missing or invalid command passed: $command")
    }
}
