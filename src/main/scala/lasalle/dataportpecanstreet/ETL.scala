package lasalle.dataportpecanstreet

import akka.typed.scaladsl.Actor
import akka.typed.scaladsl.AskPattern._
import akka.typed.{ActorRef, ActorSystem}
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import lasalle.dataportpecanstreet.RowCounter._
import lasalle.dataportpecanstreet.extract.Extract
import lasalle.dataportpecanstreet.extract.table.TableMetadata
import lasalle.dataportpecanstreet.load.Load

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


object ETL {

  val l = Logger("ETL")

  def main(args: Array[String]): Unit = {

    l.debug("Configuration settings: {}", Config.config.toString)

    val tables = Config.Etl.Extract.from
    l.info("tables: {}", tables.toString)

    // Blocking operation
    val (system, ref) = startRowCounterActorSystem()
    l.info("Row counter Actor System started")

    implicit val timeout: Timeout = Timeout(20.seconds)
    implicit val scheduler = system.scheduler

    lasalle.dataportpecanstreet.Connection.connect().map { connection =>

      val tablesMetadata = tables
        .map { tableName => tableName -> Extract.retrieveColumnMetadata(tableName, connection) }
        .map { case (tableName, metadata) => TableMetadata(tableName, metadata) }

      val res = tablesMetadata.flatMap { currentTableMetadata =>
        l.info("Table: {}", currentTableMetadata.table)
        l.info("Table columns: {}", currentTableMetadata.metadata.map(c => c.name).mkString(","))
        Load.loadMetadata(currentTableMetadata)
        Extract.guessTimeColumn(currentTableMetadata.metadata.map(_.name)).map { guessedTimeColumn =>
          l.info("Time Column: {}", guessedTimeColumn)
          Extract.customTimeIntervals.reverse.map { timeRange =>
            val res = Extract.retrieveTableData(currentTableMetadata, guessedTimeColumn, timeRange, connection)
            l.info(
              "Extracted. rows: {}; timeRange.start: {}; timeRange.end: {}"
              , res.rows.length.toString
              , timeRange.start.toString
              , timeRange.end.toString
            )
            ref ! Add(res.rows.length)
            Load.load(currentTableMetadata, res.rows)
          }
        }
      }

      // Reduce futures
      res.map(Future.sequence(_))

      val f: Future[Long] = ref ? Report

      Await.ready(f, Duration.Inf)

      f.map { res =>
        l.info("Extraction and Load done. The database should contain {} rows.", res.toString)
      }

      l.info("Program finished. Shutting down...")

      connection.close()
      System.exit(0)

    } recover {
      case t: Throwable => t.printStackTrace(System.err)
    }
  }

  private def startRowCounterActorSystem(): (ActorSystem[Nothing], ActorRef[RowCounterProtocol]) = {

    implicit val timeout: Timeout = Timeout(5.seconds)

    val root = Actor.deferred[Nothing] { ctx =>
      Actor.empty
    }

    val system = ActorSystem[Nothing]("Counter", root)
    val ref = Await.result(system.systemActorOf[RowCounterProtocol](rowCounterBehavior, "rowCounter"), Duration.Inf)

    (system, ref)

  }

}
