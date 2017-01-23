package lasalle.dataportpecanstreet

import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.JavaConverters._

/**
  * Created by vicaba on 04/10/2016.
  */
object Config {

  val config: Config = ConfigFactory.load("reference.conf")

  object Credentials {
    val username: String = config.getString("SQL.username")
    val password: String = config.getString("SQL.password")
  }

  object PostgreSqlServer {
    val hostname: String = config.getString("SQL.server.hostname")
    val port: String = config.getString("SQL.server.port")
    val database: String = config.getString("SQL.server.database")
    val schema: String = config.getString("SQL.server.schema")
  }
  
  object MongodbServer {
    val servers: List[String] = config.getStringList("mongodb.servers").asScala.toList
    val db: String = config.getString("mongodb.db")
  }

}
