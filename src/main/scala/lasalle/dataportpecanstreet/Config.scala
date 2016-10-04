package lasalle.dataportpecanstreet

import com.typesafe.config.ConfigFactory

/**
  * Created by vicaba on 04/10/2016.
  */
object Config {

  val config = ConfigFactory.load("reference.conf")

  object Credentials {
    val username = config.getString("SQL.username")
    val password = config.getString("SQL.password")
  }

  object Server {
    val hostname = config.getString("SQL.server.hostname")
    val port = config.getString("SQL.server.port")
    val database = config.getString("SQL.server.database")
    val schema = config.getString("SQL.server.schema")

  }

}
