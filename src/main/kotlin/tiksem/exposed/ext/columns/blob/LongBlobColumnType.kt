package tiksem.exposed.ext.columns.blob

class LongBlobColumnType : CustomBlobColumnType() {
    override fun sqlType(): String = "LONGBLOB"
}