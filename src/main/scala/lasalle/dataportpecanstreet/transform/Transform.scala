package lasalle.dataportpecanstreet.transform


import lasalle.dataportpecanstreet.extract.table.{DataType, TableData, TableMetadata}
import play.api.libs.json.{JsObject, Json}
import reactivemongo.bson.{BSONDateTime, BSONDocument, BSONDouble, BSONNumberLike, BSONValue}


object Transform {

  def tupleToJsonObject(tuple: TableData.Tuple): JsObject =
    tuple.map { case (field, value) => Json.obj(field -> value) }.reduce(_ ++ _)

  def tuplesToJsonObject(tuples: TableData.Tuples): List[JsObject] = tuples.map(tupleToJsonObject)

  def dataRowToBsonObject(tuples: TableData.Tuple, tableMetadata: TableMetadata): BSONDocument = {

  }


}
