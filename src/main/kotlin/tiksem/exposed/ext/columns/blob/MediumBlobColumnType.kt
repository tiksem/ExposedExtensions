package tiksem.exposed.ext.columns.blob

import org.jetbrains.exposed.sql.BlobColumnType
import org.jetbrains.exposed.sql.IColumnType

class MediumBlobColumnType : IColumnType by BlobColumnType() {
    override fun sqlType(): String = "MEDIUMBLOB"
}