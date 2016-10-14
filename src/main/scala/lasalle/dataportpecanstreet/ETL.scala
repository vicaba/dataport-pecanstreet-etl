package lasalle.dataportpecanstreet

import com.typesafe.scalalogging.Logger
import lasalle.dataportpecanstreet.extract.Extract
import lasalle.dataportpecanstreet.extract.table.TableMetadata
import lasalle.dataportpecanstreet.transform.Transform
import org.slf4j.LoggerFactory

/**
  * Created by vicaba on 04/10/2016.
  */
object ETL {

  val logger = Logger("ETL")

  def main(args: Array[String]): Unit = {
    lasalle.dataportpecanstreet.Connection.connect().map { connection =>

      val tables = Set("electricity_egauge_hours", "electricity_egauge_15min", "electricity_egauge_minutes")
      tables.foreach(println)
      println(tables.count(_ => true))
      val tablesMetadata = tables
        .map { tableName => tableName -> Extract.retrieveColumnMetadata(tableName, connection) }
        .map { case (tableName, metadata) => TableMetadata(tableName, metadata) }

      val res = tablesMetadata.flatMap { tableMetadata =>
        logger.info("Table {}", tableMetadata.table)
        Extract.guessTimeColumn(tableMetadata.metadata.map(_.name)).map { timeColumn =>
          logger.info("Time Column: {}", timeColumn)
          Extract.generateTimeIntervals(tableMetadata, timeColumn, connection).map { timeRange =>
            val res = Extract.retrieveTableData(tableMetadata, timeColumn, timeRange, connection)
            logger.info("Extracted. rows: {}; timeRange.start: {}; timeRange.end: {}", res.tableData.length.toString, timeRange.start.getTimeInMillis.toString, timeRange.end.getTimeInMillis.toString)
            Transform.tuplesToJsonObject(res.tableData)
          }
        }
      }

      println(res)

      connection.close()
    }
  }

}
