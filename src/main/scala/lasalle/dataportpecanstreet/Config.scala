package lasalle.dataportpecanstreet

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
    val EtlKey = "etl"
    val ExtractKey = "extract"
    val LoadKey = "load"

    object Extract {
      val Key = s"$EtlKey.$ExtractKey"
      val from: Set[String] = config.getStringList(s"$Key.from").asScala.toSet
      val batchInterval: Long = config.getLong(s"$Key.batchInterval")
    }

    object Load {
      val Key = s"$EtlKey.$LoadKey"
      val metadata: Boolean =  config.getBoolean(s"$Key.metadata")
    }

  }

  def execute[R](b: => Boolean)(default: R)(f: => R): R =
    if (b()) f() else default

}
