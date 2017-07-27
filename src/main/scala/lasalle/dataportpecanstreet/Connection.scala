package lasalle.dataportpecanstreet

import java.sql.{Connection => SQLConnection, DriverManager}

import lasalle.dataportpecanstreet.Config.{Credentials, PostgreSqlServer}

import scala.util.{Failure, Success, Try}


object Connection {

  val DriverName = "org.postgresql.Driver"

  def connect(): Try[SQLConnection] = {

    Try(Class.forName(DriverName)) match {
      case Failure(e) =>
        e.printStackTrace(System.err)
        Failure(e)
      case Success(_) =>
        Try {
          DriverManager.getConnection(
            s"jdbc:postgresql://${PostgreSqlServer.hostname}:${PostgreSqlServer.port}/${PostgreSqlServer.database}?currentSchema=${PostgreSqlServer.schema}",
            Credentials.username, Credentials.password)
        } match {
          case f @ Failure(e) =>
            e.printStackTrace(System.err)
            f
          case s: _ => s
        }
    }
  }

}
