package lasalle.dataportpecanstreet

import java.sql.{Connection, DriverManager}

import lasalle.dataportpecanstreet.Config.{Credentials, Server}

import scala.util.{Failure, Success, Try}


object Connection {

  val DriverName = "org.postgresql.Driver"

  def connect(): Try[Connection] = {

    Try(Class.forName(DriverName)) match {
      case Failure(e) =>
        println("Driver not found")
        e.printStackTrace(System.err)
        Failure(e)
      case Success(_) =>
        Try {
          DriverManager.getConnection(
            s"jdbc:postgresql://${Server.hostname}:${Server.port}/${Server.database}?currentSchema=${Server.schema}",
            Credentials.username, Credentials.password)
        } match {
          case f @ Failure(e) =>
            println("Can't establish connection")
            e.printStackTrace(System.err)
            f
          case s: Success[Connection] => s
        }
    }
  }

}
