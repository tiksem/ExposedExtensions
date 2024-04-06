package tiksem.exposed.ext.columns

import org.jetbrains.exposed.sql.ColumnType
import org.jetbrains.exposed.sql.IColumnType
import org.jetbrains.exposed.sql.statements.api.PreparedStatementApi
import java.sql.ResultSet

class GeneratedColumnType(
    private val subType: IColumnType,
    private val function: String
) : ColumnType() {
    override fun nonNullValueToString(value: Any): String {
        return subType.nonNullValueToString(value)
    }

    override fun notNullValueToDB(value: Any): Any {
        return subType.notNullValueToDB(value)
    }

    override fun readObject(rs: ResultSet, index: Int): Any? {
        return subType.readObject(rs, index)
    }

    override fun setParameter(stmt: PreparedStatementApi, index: Int, value: Any?) {
        subType.setParameter(stmt, index, value)
    }

    override fun validateValueBeforeUpdate(value: Any?) {
        throw IllegalArgumentException("Update is not supported for generated columns")
    }

    override fun valueFromDB(value: Any): Any {
        return subType.valueFromDB(value)
    }

    override fun valueToDB(value: Any?): Any? {
        return subType.valueToDB(value)
    }

    override fun valueToString(value: Any?): String {
        return subType.valueToString(value)
    }

    override fun sqlType(): String {
        return "${subType.sqlType()} generated always as ($function)"
    }
}