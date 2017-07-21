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

  val l = Logger("ETL")

  def main(args: Array[String]): Unit = {
    lasalle.dataportpecanstreet.Connection.connect().map { connection =>

      l.info("Configuration settings: {}", Config.config.toString)

      val tables = Config.Extract.from
      l.info("tables: {}", tables.toString)

      val tablesMetadata = tables
        .map { tableName => tableName -> Extract.retrieveColumnMetadata(tableName, connection) }
        .map { case (tableName, metadata) => TableMetadata(tableName, metadata) }

      val res = tablesMetadata.flatMap { currentTableMetadata =>
        l.info("Table: {}", currentTableMetadata.table)
        l.info("Table columns: {}", currentTableMetadata.metadata.map(c => c.name).mkString(","))
        Extract.guessTimeColumn(currentTableMetadata.metadata.map(_.name)).map { guessedTimeColumn =>
          l.info("Time Column: {}", guessedTimeColumn)
          Extract.customTimeIntervals.reverse.map { timeRange =>
            val res = Extract.retrieveTableData(currentTableMetadata, guessedTimeColumn, timeRange, connection)
            l.info(
              "Extracted. rows: {}; timeRange.start: {}; timeRange.end: {}"
              , res.tableData.length.toString
              , timeRange.start.toString
              , timeRange.end.toString
            )
            val bson = Transform.rowsToBsonDocument(res.tableData)(currentTableMetadata)
            Load.load(currentTableMetadata, bson)
          }
        }
      }

      res.map(Future.sequence(_))
      l.info("Program finished")

      connection.close()
    } recover {
      case t: Throwable => t.printStackTrace(System.err)
    }
  }

}
