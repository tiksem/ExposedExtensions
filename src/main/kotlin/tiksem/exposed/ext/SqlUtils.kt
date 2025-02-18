package tiksem.exposed.ext

import com.kotlinspirit.core.Rule
import com.kotlinspirit.core.Rules.char
import com.kotlinspirit.ext.replaceAll
import com.kotlinspirit.grammar.Grammar
import org.intellij.lang.annotations.Language
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.javatime.JavaLocalDateColumnType
import org.jetbrains.exposed.sql.javatime.JavaLocalDateTimeColumnType
import org.jetbrains.exposed.sql.javatime.JavaLocalTimeColumnType
import org.jetbrains.exposed.sql.statements.InsertStatement
import org.jetbrains.exposed.sql.statements.api.PreparedStatementApi
import org.jetbrains.exposed.sql.transactions.TransactionManager
import tiksem.ext.lists.withoutLast
import tiksem.ext.resource.FileResourceUtil
import java.io.FileNotFoundException
import java.lang.UnsupportedOperationException
import java.sql.ResultSet
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.concurrent.ConcurrentHashMap

object SqlUtils {
    private val sqlFileCache: MutableMap<String, String> = ConcurrentHashMap()

    private val argParser = object : Grammar<String>() {
        override var result: String = ""

        override fun defineRule(): Rule<*> {
            val alnum = char('a'..'z', 'A'..'Z', '0'..'9')
            return char(':') + (+alnum) {
                result = it.toString()
            }
        }
    }.toRule().compile()

    private fun <T : Any> transformArgs(sql: String, args: Map<String, T>, argsList: MutableList<T>): String {
        return argParser.replaceAll(
            source = sql,
            replacementProvider = {
                val arg = args[it]
                    ?: throw IllegalStateException("$sql sql execution failed, Argument $it is missing in getStatement")
                argsList.add(arg)
                "?"
            }
        ).toString()
    }

    private fun <T : Any> getStatement(sql: String, args: Map<String, T>): PreparedStatementApi  {
        val argsList = arrayListOf<T>()
        val newSql = transformArgs(sql, args, argsList)

        return getStatement(newSql, argsList)
    }

    private fun <T: Any> List<T>.toExposedArgs(): Iterable<Pair<IColumnType<*>, Any?>> {
        fun Any.toType(): IColumnType<*> {
            return when(this) {
                is Int -> IntegerColumnType()
                is Long -> LongColumnType()
                is Float -> FloatColumnType()
                is Double -> DoubleColumnType()
                is Boolean -> BooleanColumnType()
                is Short -> ShortColumnType()
                is CharSequence -> TextColumnType()
                is LocalDate -> JavaLocalDateColumnType()
                is LocalTime -> JavaLocalTimeColumnType()
                is LocalDateTime -> JavaLocalDateTimeColumnType()
                else -> throw UnsupportedOperationException(
                    "Unsupported columnType: ${this.javaClass.name}, " +
                            "Int, Long, Float, Double, Boolean, Short, " +
                            "CharSequence, LocalDate, LocalTime, LocalDateTime are supported"
                )
            }
        }

        return map {
            it.toType() to it
        }
    }

    private fun <T : Any> getStatement(sql: String, args: List<T>): PreparedStatementApi  {
        val conn = TransactionManager.current().connection
        val statement = conn.prepareStatement(sql, false)

        val exposedArgs = args.toExposedArgs().toList()
        statement.fillParameters(
            exposedArgs
        )

        var index = 0
        val sqlForLogging = sql.replaceAll(char('?'), replacementProvider = {
            val arg = exposedArgs[index++]
            arg.first.valueToString(arg.second as Nothing?)
        })
        exposedLogger.debug("getStatement created with sql: $sqlForLogging")

        return statement
    }

    fun <T : Any> executeSqlQuery(@Language("SQL") sql: String, args: List<T>): ResultSet {
        return getStatement(sql, args).executeQuery()
    }

    fun executeSqlQuery(@Language("SQL") sql: String): ResultSet {
        return executeSqlQuery(sql, emptyList())
    }

    fun <T : Any> executeSqlQuery(@Language("SQL") sql: String, args: Map<String, T>): ResultSet {
        return getStatement(sql, args).executeQuery()
    }

    private fun getSqlFromFile(sqlFile: String): String {
        return sqlFileCache[sqlFile] ?: FileResourceUtil.getResourceAsText(sqlFile)?.also {
            sqlFileCache[sqlFile] = it
        } ?: throw FileNotFoundException(sqlFile)
    }

    fun <T : Any> executeSqlFileQuery(sqlFile: String, args: Map<String, T>): ResultSet {
        val sql = getSqlFromFile(sqlFile)
        return getStatement(sql, args).executeQuery()
    }

    fun <T : Any> executeSqlFileQuery(
        sqlFile: String,
        args: Map<String, T>,
        columns: List<Column<*>>
    ): List<ResultRow> {
        val resultSet = executeSqlFileQuery(
            sqlFile = sqlFile,
            args = args
        )

        return resultSet.toResultRowList(columns = columns)
    }

    fun <T : Any> executeSqlUpdate(@Language("SQL") sql: String, args: List<T>): Int {
        return getStatement(sql, args).executeUpdate()
    }

    fun executeSqlUpdate(@Language("SQL") sql: String): Int {
        return getStatement(sql, emptyList()).executeUpdate()
    }

    fun <T : Any> executeSqlScript(@Language("SQL") sql: String, args: Map<String, T>) {
        val newSql = argParser.replaceAll(sql, replacementProvider = {
            val arg = args[it] ?: throw IllegalStateException("$sql sql execution failed, Argument $it is missing")
            if (arg is CharSequence) {
                "'${arg.toString().replace("'", "''")}'"
            } else {
                arg.toString()
            }
        })

        TransactionManager.current().execInBatch(newSql.split(";").let {
            if (it.size > 1) {
                it.withoutLast()
            } else {
                it
            }
        })
    }

    fun <T : Any> executeSqlFile(sqlFile: String, args: Map<String, T>) {
        val sql = getSqlFromFile(sqlFile)
        return executeSqlScript(sql, args)
    }

    fun checkTableExists(table: String): Boolean {
        val result = executeSqlQuery(
            sql = "SELECT COUNT(*) as count FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' " +
                    "           AND TABLE_NAME=? LIMIT 1",
            args = listOf(table)
        )
        return result.next() && result.getInt(1) > 0
    }

    fun getActualIndexNames(tableName: String): Set<String> {
        val resultSet = executeSqlQuery("SHOW INDEXES FROM $tableName", args = listOf())
        return HashSet<String>().apply {
            while (resultSet.next()) {
                val indexName = resultSet.getString("Key_name")
                add(indexName)
            }
        }
    }

    fun getActualColumnNames(tableName: String): Set<String> {
        val resultSet = executeSqlQuery("SHOW COLUMNS FROM $tableName", args = emptyList())
        return HashSet<String>().apply {
            while (resultSet.next()) {
                val fieldName = resultSet.getString("Field")
                add(fieldName)
            }
        }
    }

    fun dropIndex(indexName: String, tableName: String) {
        executeSqlUpdate(sql = "DROP INDEX $indexName ON $tableName", args = emptyList())
    }

    fun dropColumn(columnName: String, tableName: String) {
        executeSqlUpdate(sql = "ALTER TABLE $tableName DROP $columnName", args = emptyList())
    }

    fun getColumnTypes(tableName: String): Map<String, String> {
        val set = executeSqlQuery(sql = "SHOW COLUMNS FROM $tableName", args = emptyList())
        val indexOfName = set.findColumn("Field")
        val indexOfType = set.findColumn("Type")
        val result = LinkedHashMap<String, String>()
        while (set.next()) {
            result[set.getString(indexOfName)] = set.getString(indexOfType)
        }

        return result
    }
}

fun List<DdlAware>.executeStatements(statementsFactory: DdlAware.() -> List<String>) {
    forEach { table ->
        val statement = table.statementsFactory()
        statement.forEach {
            SqlUtils.executeSqlUpdate(sql = it, args = emptyList())
        }
    }
}

val <T : Any> InsertStatement<T>.first: ResultRow?
    get() = resultedValues?.takeIf { insertedCount > 0 }?.firstOrNull()

fun ResultSet.toResultRowList(columns: List<Column<*>>): List<ResultRow> {
    val indexes = HashMap<Expression<*>, Int>()
    for (column in columns) {
        indexes[column] = findColumn(column.name) - 1
    }

    val result = arrayListOf<ResultRow>()
    while (next()) {
        ResultRow.create(this, fieldsIndex = indexes).also {
            result.add(it)
        }
    }

    return result
}

