package lasalle.dataportpecanstreet

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.typesafe.config.{ConfigFactory, Config => Conf}

import scala.collection.JavaConverters._

object Config {

  val config: Conf = ConfigFactory.load()

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

  object Etl {
    val EtlKey: String = "etl"
    val ExtractKey: String = "extract"
    val LoadKey: String = "load"

    private def localDateTimeFromString(str: String): LocalDateTime = {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      LocalDateTime.parse(str, formatter)
    }

    object Extract {
      val Key: String = s"$EtlKey.$ExtractKey"
      val from: String = config.getString(s"$Key.from")
      val batchInterval: Long = config.getLong(s"$Key.batchInterval")
      val fromTimestamp: LocalDateTime = localDateTimeFromString(config.getString(s"$Key.fromTimestamp"))
      val toTimestamp: LocalDateTime = localDateTimeFromString(config.getString(s"$Key.toTimestamp"))
    }

    object Load {
      val Key: String = s"$EtlKey.$LoadKey"
      val to: String = config.getString(s"$Key.to")
      val metadata: Boolean = config.getBoolean(s"$Key.metadata")
    }

  }

  def execute[R](b: => Boolean)(default: R)(f: => R): R =
    if (b) f else default

}
