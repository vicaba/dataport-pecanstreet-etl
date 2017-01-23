package lasalle.dataportpecanstreet.load

import lasalle.dataportpecanstreet.Config
import reactivemongo.api.{DB, DefaultDB, MongoDriver}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits._

object MongoEnvironment {

  val driver = MongoDriver()

  val mainDb: DB = {
    val connection = driver.connection(Config.MongodbServer.servers)
    Await.result(connection.database(Config.MongodbServer.db), 2 seconds)
  }

}
