package lasalle.dataportpecanstreet

import lasalle.dataportpecanstreet.extract.Extract
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
      val tablesColumnMetadata = Extract.retrieveTableColumnMetadata(tables, connection)



      tablesColumnMetadata.map { tableColumnMetadata =>
        Extract.guessTimeColumn(tableColumnMetadata.metadata).map { timeColumn =>
          Extract.generateTimeIntervals(tableColumnMetadata, timeColumn, connection).map { timeRange =>
            val res = Extract.retrieveTableData(tableColumnMetadata, timeColumn, timeRange, connection)
            println(Transform.dataRowsToJsonObject(res.tableData))
            res
          }
        }

      }

      //iterateOverResultSet(resultSet, List(), (r, _: List[Any]) => { println(r.getTimestamp("localhour")); List() })

      connection.close()
    }
  }

}
