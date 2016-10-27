package lasalle.dataportpecanstreet.transform


import java.time.{LocalDateTime, ZoneOffset}
import java.util.Calendar

import com.typesafe.scalalogging.Logger
import lasalle.dataportpecanstreet.extract.table.{ColumnMetadata, DataType, TableData, TableMetadata}
import play.api.libs.json.{JsObject, Json}
import reactivemongo.bson.{BSONDateTime, BSONDocument, BSONDouble, BSONInteger, BSONNumberLike, BSONString, BSONValue}


object Transform {

  val logger = Logger("Transform")

  /**
    *
    * @param row
    * @return Every value is a String
    */
  def rowToJsonObject(row: TableData.Row): JsObject =
    row.map { case (field, value) =>
      if (value.isEmpty) Json.obj() else Json.obj(field -> value.get.toString)
    }.reduce(_ ++ _)


  def rowsToJsonObject(rows: TableData.Rows): List[JsObject] = rows.map(rowToJsonObject)

  def rowToBsonDocument(row: TableData.Row)(tableMetadata: TableMetadata): BSONDocument =
    row.map {
      case (field, value) => tupleToBsonDocument(tableMetadata, field, value)
    }.reduce(_ ++ _)

  def rowsToBsonDocument(rows: TableData.Rows)(tableMetadata: TableMetadata): List[BSONDocument] = rows.map(rowToBsonDocument(_)(tableMetadata))



  def tupleToBsonDocument(tableMetadata: TableMetadata, field: String, value: TableData.Value): BSONDocument =
    (for {
      cm <- tableMetadata.columnMetadataForFieldName(field)
      v <- value
    } yield {
      BSONDocument(field -> getFieldWithDataType(cm._type, v))
    }).getOrElse(BSONDocument())


  def getFieldWithDataType(dataType: DataType, value: Any): BSONValue = dataType match {
      case DataType.Integer => BSONInteger(value.asInstanceOf[Int])
      case DataType.Decimal => BSONDouble(value.asInstanceOf[Double])
      case DataType.Timestamp => BSONDateTime(value.asInstanceOf[LocalDateTime].toInstant(ZoneOffset.ofTotalSeconds(0)).toEpochMilli)
      case _ => BSONString(value.toString)
    }

}
