package tiksem.exposed.ext

import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.Table
import tiksem.ext.lists.contains

object SqlMigrationUtils {
    private fun migrateExposedIntEnumsToMysqlEnums(tables: Array<Table>) {
        for (table in tables) {
            val tableName = table.tableName
            val currentColumns = try {
                SqlUtils.getColumnTypes(tableName = tableName)
            } catch (e: Exception) {
                continue
            }

            for (column in table.columns) {
                val columnType = column.columnType
                val columnName = column.name
                val currentColumnType = currentColumns[columnName]

                // Check if the same column has int type now
                if (
                    columnType.sqlType().contains("enum(", ignoreCase = true) &&
                    currentColumnType.contentEquals("int", ignoreCase = true)
                ) {
                    // Increment enum values by 1, cause Mysql enums start with 1
                    SqlUtils.executeSqlUpdate(
                        sql = "UPDATE $tableName SET $columnName=$columnName + 1"
                    )
                }
            }
        }
    }

    fun updateTables(tables: Array<Table>) {
        migrateExposedIntEnumsToMysqlEnums(tables)
        SchemaUtils.createMissingTablesAndColumns(*tables)
        for (table in tables) {
            val tableName = table.tableName
            for (column in table.columns) {
                val statement = column.modifyStatement()
                statement.forEach {
                    SqlUtils.executeSqlUpdate(sql = it, args = emptyList())
                }
            }

            val actualIndexNames = SqlUtils.getActualIndexNames(tableName)
            for (index in table.indices) {
                if (!actualIndexNames.contains(index.indexName)) {
                    index.createStatement().forEach {
                        SqlUtils.executeSqlUpdate(sql = it, args = emptyList())
                    }
                }
            }

            val actualColumnNames = SqlUtils.getActualColumnNames(tableName)
            for (columnName in actualColumnNames) {
                if (!table.columns.contains { it.name == columnName }) {
                    SqlUtils.dropColumn(columnName = columnName, tableName = tableName)
                }
            }

            for (indexName in actualIndexNames) {
                if (!table.hasIndex(indexName)) {
                    SqlUtils.dropIndex(indexName = indexName, tableName = tableName)
                }
            }
        }
    }
}