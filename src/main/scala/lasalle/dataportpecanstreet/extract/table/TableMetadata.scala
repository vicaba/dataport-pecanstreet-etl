package lasalle.dataportpecanstreet.extract.table

import scala.collection.mutable

case class TableMetadata(table: String, metadata: Iterable[ColumnMetadata]) {

  var fieldNameToColumnMetadata: scala.collection.mutable.Map[String, ColumnMetadata] = _

  def columnMetadataForFieldName(field: String): Option[ColumnMetadata] = {
    if (fieldNameToColumnMetadata == null) fieldNameToColumnMetadata = mutable.Map[String, ColumnMetadata]()
    val columnMetadataOpt = fieldNameToColumnMetadata.get(field)
    if (columnMetadataOpt.nonEmpty) {
      columnMetadataOpt
    } else {
      metadata.find(_.name == field).map { cm =>
        fieldNameToColumnMetadata + (field -> cm)
        cm
      }
    }
  }
}