package tiksem.exposed.ext.columns

import org.jetbrains.exposed.sql.ColumnType
import org.jetbrains.exposed.sql.IColumnType
import org.jetbrains.exposed.sql.statements.api.PreparedStatementApi
import java.sql.ResultSet

class GeneratedColumnType<T>(
    private val subType: IColumnType<T>,
    private val function: String,
    private val stored: Boolean = true,
    // carry-through the underlying nullability
    override var nullable: Boolean = (subType as? ColumnType<T>)?.nullable ?: true
) : ColumnType<T>() {

    override fun sqlType(): String {
        // MySQL/MariaDB: "GENERATED ALWAYS AS (...) STORED|VIRTUAL"
        // (Exposed adds NULL/NOT NULL outside of sqlType)
        val storage = if (stored) "STORED" else "VIRTUAL"
        return "${subType.sqlType()} GENERATED ALWAYS AS ($function) $storage"
    }

    // ---- Reads delegate to the underlying type ----
    override fun readObject(rs: ResultSet, index: Int): Any? = subType.readObject(rs, index)
    override fun valueFromDB(value: Any): T? = subType.valueFromDB(value)
    override fun parameterMarker(value: T?): String = subType.parameterMarker(value)
    override fun valueAsDefaultString(value: T?): String = subType.valueAsDefaultString(value)
    override fun valueToString(value: T?): String = subType.valueToString(value)

    // ---- Writes: either delegate or block (recommended to block) ----
    // If you prefer to *allow* writes (DB will ignore/compute), keep delegating setParameter/valueToDB.
    // To fail fast on accidental writes, throw:
    override fun setParameter(stmt: PreparedStatementApi, index: Int, value: Any?) {
        // Prevent inserting/updating a generated column
        throw UnsupportedOperationException("Cannot set value for a generated column")
    }

    override fun valueToDB(value: T?): Any? {
        // Prevent inserting/updating a generated column
        throw UnsupportedOperationException("Cannot write value for a generated column")
    }
}
