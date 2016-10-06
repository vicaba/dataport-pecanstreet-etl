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
      val tables = Set("electricity_egauge_hours", "electricity_egauge_15min", "electricity_egauge_minutes")
      tables.foreach(println)
      println(tables.count(_ => true))
      val tableColumns = retrieveTableColumns(tables, connection)
      println(tableColumns)



      //iterateOverResultSet(resultSet, List(), (r, _: List[Any]) => { println(r.getTimestamp("localhour")); List() })
    }
  }

  def retrieveTableNames(connection: Connection): Set[String] = {

    val TableNameColumn = "TABLE_NAME"

    def tableReader(resultSet: ResultSet, accum: Set[String]): Set[String] = {
      Try(resultSet.getString(TableNameColumn)) match {
        case Success(tableName) => accum + tableName
        case Failure(e) =>
          e.printStackTrace(System.err)
          accum
      }
    }

    val metadata = connection.getMetaData
    val types = Array("VIEW")
    Try(metadata.getTables(null, null, null, types)).map { resultSet =>
      iterateOverResultSet(resultSet, Set[String](), tableReader)
    }.getOrElse(Set())
  }

  type TablesMetadata = Map[String, Iterable[String]]

  def retrieveTableColumns(tableNames: Iterable[String], connection: Connection): TablesMetadata = {

    val ColumnNameColumn = "column_name"

    val tableColumnQuery = (table: String) =>
      s"select $ColumnNameColumn " +
        s"from information_schema.columns " +
        s"where table_schema = 'university' and table_name = '${table}'"

    def tableColumnReader(resultSet: ResultSet, accum: Set[String]): Set[String] = {
      Try(resultSet.getString(ColumnNameColumn)) match {
        case Success(columnName) => accum + columnName
        case Failure(e) =>
          e.printStackTrace(System.err)
          accum
      }
    }

    tableNames.map { tableName =>
      val statement = connection.createStatement()

      val resultSet = statement.executeQuery(tableColumnQuery(tableName))
      tableName -> iterateOverResultSet(resultSet, Set[String](), tableColumnReader)
    }.toMap

  }

  case class TableMetadata(table: String, metadata: Iterable[String])

  case class TableData(table: String, tableData: Map[String, String])

  def retrieveTableData(tableMetadata: TableMetadata, connection: Connection): Unit = {


    val endTimeQuery = (table: String, timeColumn: String) =>
      s"select * " +
        s"from $table " +
        s"order by $timeColumn DESC"


    val tableDataQuery = (table: String) =>
      s"select * " +
        s"from $table " +
        s"where "

    def guessTimeColumn(columns: Iterable[String]): Option[String] = columns.find(_.startsWith("local"))

    guessTimeColumn(tableMetadata.metadata) match {
      case _ => None
      case Some(column) =>
    }

    def retrieveStartTime(table: String, timeColumn: String): Unit = {

      val startTimeQuery =
        s"select * " +
          s"from $table " +
          s"order by $timeColumn ASC"
      
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery(startTimeQuery)
    }

  }


}
