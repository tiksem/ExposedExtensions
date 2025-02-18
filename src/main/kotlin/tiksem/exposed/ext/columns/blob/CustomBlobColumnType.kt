package tiksem.exposed.ext.columns.blob

import org.jetbrains.exposed.sql.ColumnType
import org.jetbrains.exposed.sql.Op
import org.jetbrains.exposed.sql.QueryBuilder
import org.jetbrains.exposed.sql.statements.api.ExposedBlob
import org.jetbrains.exposed.sql.statements.api.PreparedStatementApi
import org.jetbrains.exposed.sql.vendors.PostgreSQLDialect
import org.jetbrains.exposed.sql.vendors.SQLServerDialect
import org.jetbrains.exposed.sql.vendors.currentDialect
import java.io.InputStream
import java.sql.Blob
import java.sql.ResultSet

internal object NULL : Op<Any>() {
    override fun toQueryBuilder(queryBuilder: QueryBuilder): Unit = queryBuilder {
        append("NULL")
    }
}

abstract class CustomBlobColumnType(
    /** Returns whether an OID column should be used instead of BYTEA. This value only applies to PostgreSQL databases. */
    val useObjectIdentifier: Boolean = false
) : ColumnType<ExposedBlob>() {
    abstract override fun sqlType(): String

    override fun valueFromDB(value: Any): ExposedBlob = when (value) {
        is ExposedBlob -> value
        is InputStream -> ExposedBlob(value)
        is ByteArray -> ExposedBlob(value)
        is Blob -> ExposedBlob(value.binaryStream)
        else -> error("Unexpected value of type Blob: $value of ${value::class.qualifiedName}")
    }

    override fun nonNullValueToString(value: ExposedBlob): String = currentDialect.dataTypeProvider.hexToDb(value.hexString())

    override fun readObject(rs: ResultSet, index: Int) = when {
        currentDialect is SQLServerDialect -> rs.getBytes(index)?.let(::ExposedBlob)
        currentDialect is PostgreSQLDialect && useObjectIdentifier -> rs.getBlob(index)?.binaryStream?.let(::ExposedBlob)
        else -> rs.getBinaryStream(index)?.let(::ExposedBlob)
    }

    override fun setParameter(stmt: PreparedStatementApi, index: Int, value: Any?) {
        when (val toSetValue = (value as? ExposedBlob)?.inputStream ?: value) {
            is InputStream -> stmt.setInputStream(index, toSetValue, useObjectIdentifier)
            null, is NULL -> stmt.setNull(index, this)
            else -> super.setParameter(stmt, index, toSetValue)
        }
    }
}