package lasalle.dataportpecanstreet.extract


import java.sql.{Connection, ResultSet, Timestamp}
import java.util.{Calendar, Date}

import lasalle.dataportpecanstreet.Config
import lasalle.dataportpecanstreet.extract.table.{ColumnMetadata, DataType, TableData, TableMetadata}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}


object Extract {

  case class TimeRange(start: Calendar, end: Calendar)

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

  def retrieveColumnMetadata(tableName: String, connection: Connection): Iterable[ColumnMetadata] = {

    val ColumnNameColumn = "column_name"
    val DataTypeColumn = "data_type"

    val tableColumnQuery = (table: String) =>
      s"select $ColumnNameColumn, $DataTypeColumn " +
        s"from information_schema.columns " +
        s"where table_schema = '${Config.Server.schema}' and table_name = '${table}'"

    def tableColumnReader(resultSet: ResultSet, accum: Set[ColumnMetadata]): Set[ColumnMetadata] =
      (for {
        columnName <- Try(resultSet.getString(ColumnNameColumn))
        dataType <- Try(resultSet.getString(DataTypeColumn))
      } yield {
        accum + ColumnMetadata(columnName, DataType.Undefined(dataType))
      }).getOrElse(Set[ColumnMetadata]())

    val statement = connection.createStatement()

    val resultSet = statement.executeQuery(tableColumnQuery(tableName))
    val res = iterateOverResultSet(resultSet, Set[ColumnMetadata](), tableColumnReader)
    println(res)
    res
  }

  def guessTimeColumn(columns: Iterable[String]): Option[String] = columns.find(_.startsWith("local"))

  def generateTimeIntervals(tableMetadata: TableMetadata, timeColumn: String, connection: Connection): List[TimeRange] = {

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

      timeSlices.sliding(2).toList.map(l => TimeRange(l.head, l.last))

    }).getOrElse(List[TimeRange]())
  }

  def retrieveTableData(tableMetadata: TableMetadata, timeColumn: String, timeRange: TimeRange, connection: Connection): TableData = {

    def tableDataReader(resultSet: ResultSet, accum: TableData.Registers): TableData.Registers = {
      accum :+ tableMetadata.metadata.map { field =>
        field.name -> resultSet.getString(field.name)
      }.toMap
    }


    val startDate = new java.sql.Date(timeRange.start.getTimeInMillis)
    val endDate = new java.sql.Date(timeRange.end.getTimeInMillis)

    val statement = connection.createStatement()

    val query = s"select * " +
      s"from ${Config.Server.schema}.${tableMetadata.table} " +
      s"where $timeColumn between '$startDate' and '$endDate'"

    val resultSet = statement.executeQuery(query)

    Try(TableData(tableMetadata, iterateOverResultSet(resultSet, TableData.registers(), tableDataReader))).recover {
      case t => t.printStackTrace(System.err)
    }

    null

  }

}
