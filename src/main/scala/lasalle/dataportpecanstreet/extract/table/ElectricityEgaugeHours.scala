package lasalle.dataportpecanstreet.extract.table

class ElectricityEgaugeHours(databaseName: String) {

  val tableName = "electricity_egauge_hours"

  object Fields {

  }

  val maxDateQuery = "select localhour from university.electricity_egauge_hours limit 1 ORDER BY localhour DESC"

}
