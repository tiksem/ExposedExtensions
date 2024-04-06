package tiksem.exposed.ext

import org.intellij.lang.annotations.Language
import org.jetbrains.exposed.sql.Column
import org.jetbrains.exposed.sql.ResultRow
import org.jetbrains.exposed.sql.Table
import tiksem.exposed.ext.columns.GeneratedColumnType
import tiksem.exposed.ext.columns.MysqlSetColumnType
import tiksem.exposed.ext.columns.TextJsonColumnType
import tiksem.ext.bitset.LongEnumBitSet
import tiksem.ext.lists.contains

fun Table.hasIndex(indexName: String): Boolean {
    return indices.contains {
        it.indexName == indexName
    } || (indexName == "PRIMARY" && primaryKey != null) || foreignKeys.contains {
        it.fkName == indexName
    }
}

inline fun <reified T : Enum<T>> Table.mysqlEnum(name: String): Column<T> {
    val enums = T::class.java.enumConstants
    return customEnumeration(
        name = name,
        sql = "ENUM(${enums.joinToString(",") { "'$it'" }})",
        fromDb = { value -> enums.find { it.toString() == value.toString() }!! },
        toDb = { it.name }
    )
}

inline fun <reified T> Table.generated(column: Column<T>, function: String): Column<T> {
    return replaceColumn(
        oldColumn = column,
        newColumn = Column(
            table = this,
            name = column.name,
            columnType = GeneratedColumnType(
                subType = column.columnType,
                function = function
            )
        )
    )
}

fun Table.sqlQuery(@Language("SQL") sql: String): List<ResultRow> {
    return SqlUtils.executeSqlQuery(sql = sql).toResultRowList(columns)
}

fun Table.textJson(name: String): Column<String> {
    return registerColumn(name, TextJsonColumnType())
}

fun <E : Enum<E>> Table.mysqlSet(name: String, enumClass: Class<E>): Column<LongEnumBitSet<E>> {
    return registerColumn(name, MysqlSetColumnType(enumClass))
}