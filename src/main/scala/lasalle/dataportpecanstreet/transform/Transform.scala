package lasalle.dataportpecanstreet.transform

import lasalle.dataportpecanstreet.extract.table.TableData
import play.api.libs.json.{JsObject, Json}


object Transform {

  def dataRowToJsonObject(register: TableData.Register): JsObject =
    register.map { case (field, value) => Json.obj(field -> value) }.reduce(_ ++ _)

  def dataRowsToJsonObject(dataRows: TableData.Registers): List[JsObject] = dataRows.map(dataRowToJsonObject)

}
