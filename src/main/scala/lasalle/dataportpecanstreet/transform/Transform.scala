package lasalle.dataportpecanstreet.transform

import lasalle.dataportpecanstreet.extract.Extract
import play.api.libs.json.{JsObject, Json}
import play.api.libs.json._


object Transform {

  def dataRowToJsonObject(dataRow: Extract.DataRow): JsObject =
    dataRow.map { case (field, value) => Json.obj(field -> value) }.reduce(_ ++ _)

  def dataRowsToJsonObject(dataRows: Extract.DataRows): List[JsObject] = dataRows.map(dataRowToJsonObject)

}
