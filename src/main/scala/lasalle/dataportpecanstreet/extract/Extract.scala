package lasalle.dataportpecanstreet.extract


import java.sql.{Connection, ResultSet, Timestamp}
import java.util.{Calendar, Date}

import lasalle.dataportpecanstreet.Config
import lasalle.dataportpecanstreet.transform.Transform

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
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
      val tablesColumnMetadata = retrieveTableColumnMetadata(tables, connection)
      println(tablesColumnMetadata)
      val tableData = retrieveTableData(tablesColumnMetadata.head, connection)
      Transform.dataRowsToJsonObject(tableData)

      //iterateOverResultSet(resultSet, List(), (r, _: List[Any]) => { println(r.getTimestamp("localhour")); List() })

      connection.close()
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


  def retrieveTableColumnMetadata(tableNames: Iterable[String], connection: Connection): Iterable[TableColumnMetadata] = {

    val ColumnNameColumn = "column_name"

    val tableColumnQuery = (table: String) =>
      s"select $ColumnNameColumn " +
        s"from information_schema.columns " +
        s"where table_schema = '${Config.Server.schema}' and table_name = '${table}'"

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
      TableColumnMetadata(tableName, iterateOverResultSet(resultSet, Set[String](), tableColumnReader))
    }

  }

  case class TableColumnMetadata(table: String, metadata: Iterable[String])

  type DataRow = Map[String, String]

  type DataRows = List[DataRow]

  case class TableData(table: String, tableData: DataRows)

  def retrieveTableData(tableMetadata: TableColumnMetadata, connection: Connection): Option[TableData] = {

    def guessTimeColumn(columns: Iterable[String]): Option[String] = columns.find(_.startsWith("local"))

    def retrieveStartTime(table: String, timeColumn: String): Option[Timestamp] = {

      val startTimeQuery =
        s"select * " +
          s"from ${Config.Server.schema}.$table " +
          s"order by $timeColumn ASC limit 1"

      val resultSet = connection.createStatement().executeQuery(startTimeQuery)
      if (resultSet.next()) Some(resultSet.getTimestamp(timeColumn)) else None

    }

    def retrieveEndTime(table: String, timeColumn: String): Option[Timestamp] = {

      val endTimeQuery =
        s"select * " +
          s"from ${Config.Server.schema}.$table " +
          s"order by $timeColumn DESC limit 1"

      val resultSet = connection.createStatement().executeQuery(endTimeQuery)
      if (resultSet.next()) Some(resultSet.getTimestamp(timeColumn)) else None

    }

    def sqlTimestampToCalendarDate(t: Timestamp) = {
      val c = Calendar.getInstance()
      c.setTime(new Date(t.getTime))
      c
    }

    def tableDataReader(resultSet: ResultSet, accum: DataRows): DataRows = {
      accum :+ tableMetadata.metadata.map { field =>
        field -> resultSet.getString(field)
      }.toMap
    }

    for {
      timeColumn <- guessTimeColumn(tableMetadata.metadata)
      startTime <- retrieveStartTime(tableMetadata.table, timeColumn).map(sqlTimestampToCalendarDate)
      endTime <- retrieveEndTime(tableMetadata.table, timeColumn).map(sqlTimestampToCalendarDate)
    } yield {
      val timeSlices = ListBuffer[Calendar]()
      timeSlices += startTime

      while (timeSlices.last.compareTo(endTime) <= 0) {
        val dateBetween = Calendar.getInstance()
        dateBetween.setTime(timeSlices.last.getTime)
        dateBetween.add(Calendar.MONTH, 1)
        timeSlices += dateBetween
      }

      case class TimeRange(start: Calendar, end: Calendar)
      val timeRanges = timeSlices.sliding(2).toList.map(l => TimeRange(l.head, l.last))

      timeRanges.take(1).map { timeRange =>

        val startDate = new java.sql.Date(timeRange.start.getTimeInMillis)
        val endDate = new java.sql.Date(timeRange.end.getTimeInMillis)

        val statement = connection.createStatement()

        val query = s"select * " +
          s"from ${Config.Server.schema}.${tableMetadata.table} " +
          s"where $timeColumn between '$startDate' and '$endDate'"

        val resultSet = statement.executeQuery(query)

        iterateOverResultSet(resultSet, List[Map[String, String]](), tableDataReader)
      }.map(TableData(tableMetadata.table, _))

    }
  }

}
