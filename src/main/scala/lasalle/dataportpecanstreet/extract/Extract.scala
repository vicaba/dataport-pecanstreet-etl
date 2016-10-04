package lasalle.dataportpecanstreet.extract



import java.sql.{Connection, ResultSet}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}


/**
  * Read http://larsho.blogspot.com.es/2008/01/integrating-with-phpmysql-application.html
  */
object Extract {

  @tailrec
    def iterateOverResultSet[R](resultSet: ResultSet, accum: R, f: (ResultSet, R) => R): R = {
      if (resultSet.next()) {
        iterateOverResultSet(resultSet, f(resultSet, accum), f)
      } else accum
    }

  def main(args: Array[String]): Unit = {
    lasalle.dataportpecanstreet.Connection.connect().map { connection =>

      val statement = connection.createStatement()
      val query = "select * from university.electricity_egauge_hours limit 3"
      val resultSet = statement.executeQuery(query)
      val tables = getTableNames(connection)
      tables.foreach(println)
      //iterateOverResultSet(resultSet, List(), (r, _: List[Any]) => { println(r.getTimestamp("localhour")); List() })
    }
  }

  def getTableNames(connection: Connection): List[String] = {

    def tableReader(resultSet: ResultSet, tableAccum: List[String]): List[String] = {
      Try(resultSet.getString("TABLE_NAME")) match {
        case Success(tableName) => tableAccum :+ tableName
        case Failure(e) =>
          e.printStackTrace(System.err)
          tableAccum
      }
    }

    val metadata = connection.getMetaData
    val types = Array("TABLE")
    val resultSet = metadata.getTables(null, null, null, types)
    iterateOverResultSet(resultSet, List(), tableReader)
  }

  def electricityEgaugeHoursTable(connection: Connection) = {

    val maxDateQuery = "select localhour from university.electricity_egauge_hours limit 1 ORDER BY localhour DESC"

  }

}
