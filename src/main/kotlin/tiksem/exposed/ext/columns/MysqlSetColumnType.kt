package tiksem.exposed.ext.columns

import org.jetbrains.exposed.sql.ColumnType
import org.jetbrains.exposed.sql.statements.api.PreparedStatementApi
import tiksem.ext.bitset.LongEnumBitSet
import tiksem.ext.strings.quote
import tiksem.ext.strings.unquote

class MysqlSetColumnType<E : Enum<E>>(private val enumClass: Class<E>) : ColumnType() {
    private val enumConstants: Array<E> = enumClass.enumConstants ?: throw IllegalArgumentException("Class must be an enum type")

    init {
        if (enumConstants.size > 64) {
            throw IllegalArgumentException("Mysql SET can contain maximum 64 items")
        }
    }

    override fun sqlType(): String = "SET(${enumConstants.joinToString { "'${it.name}'" }})"

    override fun setParameter(stmt: PreparedStatementApi, index: Int, value: Any?) {
        when (value) {
            is LongEnumBitSet<*> -> {
                stmt[index] = value.toList().joinToString(",")
            }
            is CharSequence -> {
                stmt[index] = value.toString().unquote('\'')
            }
            else -> stmt.setNull(index, this)
        }
    }

    override fun valueFromDB(value: Any): Any {
        if (value is LongEnumBitSet<*>) {
            return value
        }
        if (value is String) {
            val setValues = value.split(",").map { it.trim() }
            val bitSet = LongEnumBitSet(enumClass)
            setValues.forEach { valName ->
                enumConstants.find { it.name == valName }?.let { bitSet.add(it) }
            }
            return bitSet
        }
        throw IllegalArgumentException("Unexpected value type: ${value::class.qualifiedName}")
    }

    override fun notNullValueToDB(value: Any): Any {
        if (value is LongEnumBitSet<*>) {
            return value.toString().quote('\'')
        }
        throw IllegalArgumentException("Unexpected value type: ${value::class.qualifiedName}")
    }
}
