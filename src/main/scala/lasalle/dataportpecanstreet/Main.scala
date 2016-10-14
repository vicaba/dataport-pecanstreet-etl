package lasalle.dataportpecanstreet

import lasalle.dataportpecanstreet.extract.Extract
import lasalle.dataportpecanstreet.extract.table.TableMetadata
import lasalle.dataportpecanstreet.transform.Transform

/**
  * Created by vicaba on 04/10/2016.
  */
object Main {

  def main(args: Array[String]): Unit = {
    lasalle.dataportpecanstreet.Connection.connect().map { connection =>

      val tables = Set("electricity_egauge_hours", "electricity_egauge_15min", "electricity_egauge_minutes")
      tables.foreach(println)
      println(tables.count(_ => true))
      val tablesMetadata = tables
        .map { tableName => tableName -> Extract.retrieveColumnMetadata(tableName, connection) }
        .map { case (tableName, metadata) => TableMetadata(tableName, metadata) }

      tablesMetadata.flatMap { tableMetadata =>
        Extract.guessTimeColumn(tableMetadata.metadata.map(_.name)).map { timeColumn =>
          Extract.generateTimeIntervals(tableMetadata, timeColumn, connection).map { timeRange =>
            val res = Extract.retrieveTableData(tableMetadata, timeColumn, timeRange, connection)
            val json = Transform.dataRowsToJsonObject(res.tableData)
            println(json)
            json
          }
        }
      }

      connection.close()
    }
  }

}
