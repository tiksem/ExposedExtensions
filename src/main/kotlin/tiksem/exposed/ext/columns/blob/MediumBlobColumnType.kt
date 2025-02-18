package tiksem.exposed.ext.columns.blob

class MediumBlobColumnType : CustomBlobColumnType() {
    override fun sqlType(): String = "MEDIUMBLOB"
}