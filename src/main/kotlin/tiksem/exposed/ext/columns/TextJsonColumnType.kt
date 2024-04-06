package tiksem.exposed.ext.columns

import org.jetbrains.exposed.sql.TextColumnType

class TextJsonColumnType : TextColumnType() {
    override fun sqlType(): String {
        return "JSON"
    }
}