package tiksem.exposed.ext

import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.SqlExpressionBuilder.greater
import org.jetbrains.exposed.sql.SqlExpressionBuilder.less
import tiksem.ext.bitset.LongEnumBitSet

fun <T> Query.getSingleValue(): T? {
    val row = firstOrNull() ?: return null
    val expression = row.fieldIndex.keys.first()
    return row[expression] as T
}

fun Query.orderByIndex(index: Index, sortOrder: SortOrder): Query {
    return orderBy(
        *index.columns.map {
            it to sortOrder
        }.toTypedArray()
    )
}

fun <Id : Comparable<Id>> paginationWhere(
    dateColumn: Column<Long>,
    idColumn: Column<Id>,
    dateSeek: Long,
    idSeek: Id
): Op<Boolean> {
    return (dateColumn less dateSeek) or ((dateColumn eq dateSeek) and (idColumn less idSeek))
}

fun <Id : Comparable<Id>> FieldSet.createPaginationSelect(
    dateColumn: Column<Long>,
    idColumn: Column<Id>,
    dateSeek: Long?,
    idSeek: Id?,
    additionalWhere: Op<Boolean>? = null
): Query {
    assert((dateSeek == null && idSeek == null) || (dateSeek != null && idSeek != null))
    return if (dateSeek != null && idSeek != null) {
        select(
            where = paginationWhere(
                dateColumn = dateColumn,
                idColumn = idColumn,
                dateSeek = dateSeek,
                idSeek = idSeek
            ).let {
                if (additionalWhere != null) {
                    it and additionalWhere
                } else {
                    it
                }
            }
        )
    } else {
        if (additionalWhere == null) {
            selectAll()
        } else {
            select {
                additionalWhere
            }
        }
    }.orderBy(dateColumn to SortOrder.DESC, idColumn to SortOrder.DESC)
}

fun <T> Column<T>.selectFirst(where: SqlExpressionBuilder.() -> Op<Boolean>): T? {
    return table.slice(this).select(where).limit(1).getSingleValue<T>()
}

fun <T> Column<T>.selectAll(where: SqlExpressionBuilder.() -> Op<Boolean> = { Op.TRUE }): List<T> {
    return table.slice(this).select(where).map {
        val expression = it.fieldIndex.keys.first()
        it[expression] as T
    }
}

fun <E : Enum<E>> Column<LongEnumBitSet<E>>.findInSet(value: E): CustomFunction<Int> =
    CustomFunction("FIND_IN_SET", IntegerColumnType(), QueryParameter(value.toString(), VarCharColumnType()), this)

fun <E : Enum<E>> Column<LongEnumBitSet<E>>.contains(value: E): Op<Boolean> = findInSet(value) greater 0