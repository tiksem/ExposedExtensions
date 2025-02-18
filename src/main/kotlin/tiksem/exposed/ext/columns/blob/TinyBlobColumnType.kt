package tiksem.exposed.ext.columns.blob

class TinyBlobColumnType : CustomBlobColumnType() {
    override fun sqlType(): String = "TINYBLOB"
}