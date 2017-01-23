package lasalle.dataportpecanstreet

import com.typesafe.scalalogging.Logger
import lasalle.dataportpecanstreet.extract.Extract
import lasalle.dataportpecanstreet.extract.table.TableMetadata
import lasalle.dataportpecanstreet.extract.time.Helper
import lasalle.dataportpecanstreet.load.Load
import lasalle.dataportpecanstreet.transform.Transform

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits._

object ETL {

  val logger = Logger("ETL")

  def main(args: Array[String]): Unit = {
    lasalle.dataportpecanstreet.Connection.connect().map { connection =>

      val tables = Set("electricity_egauge_hours")
      tables.foreach(println)
      println(tables.count(_ => true))
      val tablesMetadata = tables
        .map { tableName => tableName -> Extract.retrieveColumnMetadata(tableName, connection) }
        .map { case (tableName, metadata) => TableMetadata(tableName, metadata) }

      println(Extract.customTimeIntervals)

      val res = tablesMetadata.flatMap { tableMetadata =>
        logger.info("Table {}", tableMetadata.table)
        Extract.guessTimeColumn(tableMetadata.metadata.map(_.name)).map { timeColumn =>
          logger.info("Time Column: {}", timeColumn)
          Extract.customTimeIntervals.map { timeRange =>
            val res = Extract.retrieveTableData(tableMetadata, timeColumn, timeRange, connection)
            logger.info("Extracted. rows: {}; timeRange.start: {}; timeRange.end: {}", res.tableData.length.toString, Helper.localDateTimeToMillis(timeRange.start).toString, Helper.localDateTimeToMillis(timeRange.end).toString)
            val json = Transform.rowsToBsonDocument(res.tableData)(tableMetadata)

            Load.load(tableMetadata, json)
          }
        }
      }

      val res2 = res.map(Future.sequence(_))
      res2.foreach(_.foreach(_.foreach(println)))

      connection.close()
    } recover {
      case t: Throwable => t.printStackTrace(System.err)
    }
  }

}
