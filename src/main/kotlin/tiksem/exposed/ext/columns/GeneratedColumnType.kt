package tiksem.exposed.ext.columns

import org.jetbrains.exposed.sql.ColumnType
import org.jetbrains.exposed.sql.IColumnType
import org.jetbrains.exposed.sql.statements.api.PreparedStatementApi
import java.sql.ResultSet

class GeneratedColumnType<T>(
    private val subType: IColumnType<T>,
    private val function: String
) : ColumnType<T>() {

    override fun readObject(rs: ResultSet, index: Int): Any? {
        return subType.readObject(rs, index)
    }

    override fun setParameter(stmt: PreparedStatementApi, index: Int, value: Any?) {
        subType.setParameter(stmt, index, value)
    }

    override fun sqlType(): String {
        return "${subType.sqlType()} generated always as ($function)"
    }

    override fun valueFromDB(value: Any): T? {
        return subType.valueFromDB(value)
    }

    override fun parameterMarker(value: T?): String {
        return subType.parameterMarker(value)
    }

    override fun validateValueBeforeUpdate(value: T?) {
        subType.validateValueBeforeUpdate(value)
    }

    override fun valueAsDefaultString(value: T?): String {
        return subType.valueAsDefaultString(value)
    }

    override fun valueToDB(value: T?): Any? {
        return subType.valueToDB(value)
    }

    override fun valueToString(value: T?): String {
        return subType.valueToString(value)
    }
}