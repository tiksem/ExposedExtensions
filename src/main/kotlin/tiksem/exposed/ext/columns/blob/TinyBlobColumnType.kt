package tiksem.exposed.ext.columns.blob

import org.jetbrains.exposed.sql.BlobColumnType
import org.jetbrains.exposed.sql.IColumnType

class TinyBlobColumnType : IColumnType by BlobColumnType() {
    override fun sqlType(): String = "TINYBLOB"
}