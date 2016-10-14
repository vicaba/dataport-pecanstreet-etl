package lasalle.dataportpecanstreet.transform

import lasalle.dataportpecanstreet.extract.Extract
import lasalle.dataportpecanstreet.extract.table.TableData
import play.api.libs.json.{JsObject, Json}
import play.api.libs.json._


object Transform {

  def dataRowToJsonObject(dataRow: TableData.Register): JsObject =
    dataRow.map { case (field, value) => Json.obj(field -> value) }.reduce(_ ++ _)

  def dataRowsToJsonObject(dataRows: TableData.Registers): List[JsObject] = dataRows.map(dataRowToJsonObject)

}
