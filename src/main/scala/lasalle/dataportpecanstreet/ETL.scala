package lasalle.dataportpecanstreet

import com.typesafe.scalalogging.Logger
import lasalle.dataportpecanstreet.extract.Extract
import lasalle.dataportpecanstreet.extract.table.TableMetadata
import lasalle.dataportpecanstreet.extract.time.Helper
import lasalle.dataportpecanstreet.load.{Load, MongoEnvironment}
import lasalle.dataportpecanstreet.transform.Transform

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits._

object ETL {

  val logger = Logger("ETL")

  def main(args: Array[String]): Unit = {
    lasalle.dataportpecanstreet.Connection.connect().map { connection =>

      logger.info("Configuration settings: {}", Config.config.toString)

      val tables = Set("electricity_egauge_hours")
      logger.info("tables: {}", tables.toString)

      val tablesMetadata = tables
        .map { tableName => tableName -> Extract.retrieveColumnMetadata(tableName, connection) }
        .map { case (tableName, metadata) => TableMetadata(tableName, metadata) }

      val res = tablesMetadata.flatMap { currentTableMetadata =>
        logger.info("Table: {}", currentTableMetadata.table)
        logger.info("Table columns: {}", currentTableMetadata.metadata)
        Extract.guessTimeColumn(currentTableMetadata.metadata.map(_.name)).map { guessedTimeColumn =>
          logger.info("Time Column: {}", guessedTimeColumn)
          Extract.customTimeIntervals.map { timeRange =>
            val res = Extract.retrieveTableData(currentTableMetadata, guessedTimeColumn, timeRange, connection)
            logger.info(
              "Extracted. rows: {}; timeRange.start: {}; timeRange.end: {}"
              , res.tableData.length.toString
              , Helper.localDateTimeToMillis(timeRange.start).toString
              , Helper.localDateTimeToMillis(timeRange.end).toString
            )
            val bson = Transform.rowsToBsonDocument(res.tableData)(currentTableMetadata)
            Load.load(currentTableMetadata, bson)
          }
        }
      }

      res.map(Future.sequence(_))
      logger.info("Program finished")

      connection.close()
    } recover {
      case t: Throwable => t.printStackTrace(System.err)
    }
  }

}
