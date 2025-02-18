package tiksem.exposed.ext.columns.blob

import org.jetbrains.exposed.sql.BlobColumnType
import org.jetbrains.exposed.sql.IColumnType

class LongBlobColumnType : CustomBlobColumnType() {
    override fun sqlType(): String = "LONGBLOB"
}