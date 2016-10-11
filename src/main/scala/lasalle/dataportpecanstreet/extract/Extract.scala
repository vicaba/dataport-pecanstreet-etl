package lasalle.dataportpecanstreet.extract


import java.sql.{Connection, ResultSet, Timestamp}
import java.util.{Calendar, Date}

import lasalle.dataportpecanstreet.Config

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}


object Extract {

  case class TimeRange(start: Calendar, end: Calendar)

  case class TableColumnMetadata(table: String, metadata: List[String])

  type DataRow = Map[String, String]

  type DataRows = List[DataRow]

  case class TableData(table: String, tableData: DataRows)

  @tailrec
  def iterateOverResultSet[R](resultSet: ResultSet, accum: R, f: (ResultSet, R) => R): R = {
    if (resultSet.next()) {
      iterateOverResultSet(resultSet, f(resultSet, accum), f)
    } else accum
  }

  def retrieveTableNames(connection: Connection): Set[String] = {

    val TableNameColumn = "table_name"

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

    def tableColumnReader(resultSet: ResultSet, accum: Set[String]): Set[String] =
      Try(resultSet.getString(ColumnNameColumn)) match {
        case Success(columnName) => accum + columnName
        case Failure(e) =>
          e.printStackTrace(System.err)
          accum
      }


    tableNames.map { tableName =>
      val statement = connection.createStatement()

      val resultSet = statement.executeQuery(tableColumnQuery(tableName))
      TableColumnMetadata(tableName, iterateOverResultSet(resultSet, Set[String](), tableColumnReader).toList)
    }

  }

  def guessTimeColumn(columns: Iterable[String]): Option[String] = columns.find(_.startsWith("local"))

  def generateTimeIntervals(tableColumnMetadata: TableColumnMetadata, timeColumn: String, connection: Connection): List[TimeRange] = {

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

    (for {
      startTime <- retrieveStartTime(tableColumnMetadata.table, timeColumn).map(sqlTimestampToCalendarDate)
      endTime <- retrieveEndTime(tableColumnMetadata.table, timeColumn).map(sqlTimestampToCalendarDate)
    } yield {

      val timeSlices = ListBuffer[Calendar]()
      timeSlices += startTime

      while (timeSlices.last.compareTo(endTime) <= 0) {
        val dateBetween = Calendar.getInstance()
        dateBetween.setTime(timeSlices.last.getTime)
        dateBetween.add(Calendar.MONTH, 1)
        timeSlices += dateBetween
      }

      timeSlices.sliding(2).toList.map(l => TimeRange(l.head, l.last))

    }).getOrElse(List[TimeRange]())
  }

  def retrieveTableData(tableMetadata: TableColumnMetadata, timeColumn: String, timeRange: TimeRange, connection: Connection): TableData = {

    def tableDataReader(resultSet: ResultSet, accum: DataRows): DataRows = {
      accum :+ tableMetadata.metadata.map { field =>
        field -> resultSet.getString(field)
      }.toMap
    }


    val startDate = new java.sql.Date(timeRange.start.getTimeInMillis)
    val endDate = new java.sql.Date(timeRange.end.getTimeInMillis)

    val statement = connection.createStatement()

    val query = s"select * " +
      s"from ${Config.Server.schema}.${tableMetadata.table} " +
      s"where $timeColumn between '$startDate' and '$endDate'"

    val resultSet = statement.executeQuery(query)

    TableData(tableMetadata.table, iterateOverResultSet(resultSet, List[Map[String, String]](), tableDataReader))

  }

}
