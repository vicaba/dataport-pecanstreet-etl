package lasalle.dataportpecanstreet.extract


import java.sql.{Connection, ResultSet, Timestamp}
import java.time.temporal.ChronoUnit
import java.time.{Duration, LocalDate, LocalDateTime, Month, Period, ZoneId, ZoneOffset}
import java.util
import java.util.{Calendar, Date}

import com.typesafe.scalalogging.Logger
import lasalle.dataportpecanstreet.Config
import lasalle.dataportpecanstreet.extract.table.{ColumnMetadata, DataType, TableData, TableMetadata}
import lasalle.dataportpecanstreet.extract.time.{DateRange, Helper, TimeRange}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}


object Extract {

  val logger = Logger("Extract")

  val NullValueAsString = "null"

  def postgresDataType2DomainType(dataType: String): DataType = dataType match {
    case "integer" => DataType.Integer
    case "numeric" => DataType.Decimal
    case "timestamp with time zone" => DataType.Timestamp
    case "timestamp without time zone" => DataType.Timestamp
    case _ => DataType.String
  }

  def getFieldWithDataType(fieldName: String, dataType: DataType, resultSet: ResultSet): TableData.Value = {

    def sqlTimestampToCalendarDateOrNull(t: Timestamp) = if (t == null) t else Helper.sqlTimestampToLocalTimeDate(t)

    dataType match {
      case DataType.Integer => Option(resultSet.getInt(fieldName))
      case DataType.Decimal => Option(resultSet.getBigDecimal(fieldName))
      case DataType.Timestamp => Option(sqlTimestampToCalendarDateOrNull(resultSet.getTimestamp(fieldName)))
      case _ => Option(resultSet.getString(fieldName)).filterNot(_ == NullValueAsString)
    }
  }

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
        s"where table_schema = '${Config.PostgreSqlServer.schema}' and table_name = '${table}'"

    def tableColumnReader(resultSet: ResultSet, accum: Set[ColumnMetadata]): Set[ColumnMetadata] =
      (for {
        columnName <- Try(resultSet.getString(ColumnNameColumn))
        dataType <- Try(resultSet.getString(DataTypeColumn))
      } yield {
        accum + ColumnMetadata(columnName, postgresDataType2DomainType(dataType))
      }).getOrElse(Set[ColumnMetadata]())

    val statement = connection.createStatement()

    val resultSet = statement.executeQuery(tableColumnQuery(tableName))
    val res = iterateOverResultSet(resultSet, Set[ColumnMetadata](), tableColumnReader)
    res
  }

  def guessTimeColumn(columns: Iterable[String]): Option[String] = columns.find(_.startsWith("local"))

  def generateTimeIntervals(tableMetadata: TableMetadata, timeColumn: String, connection: Connection): List[TimeRange] = {

    def retrieveStartTime(table: String, timeColumn: String): Option[Timestamp] = {

      val startTimeQuery =
        s"select * " +
          s"from ${Config.PostgreSqlServer.schema}.$table " +
          s"order by $timeColumn ASC limit 1"

      val resultSet = connection.createStatement().executeQuery(startTimeQuery)
      if (resultSet.next()) Some(resultSet.getTimestamp(timeColumn)) else None

    }

    def retrieveEndTime(table: String, timeColumn: String): Option[Timestamp] = {

      val endTimeQuery =
        s"select * " +
          s"from ${Config.PostgreSqlServer.schema}.$table " +
          s"order by $timeColumn DESC limit 1"

      val resultSet = connection.createStatement().executeQuery(endTimeQuery)
      if (resultSet.next()) Some(resultSet.getTimestamp(timeColumn)) else None

    }

    (for {
      startTime <- retrieveStartTime(tableMetadata.table, timeColumn).map(Helper.sqlTimestampToLocalTimeDate)
      endTime <- retrieveEndTime(tableMetadata.table, timeColumn).map(Helper.sqlTimestampToLocalTimeDate)
    } yield {
      TimeRange(startTime, endTime).slice(Period.ofMonths(1))
    }).getOrElse(List[TimeRange]())
  }

  def retrieveTableData(tableMetadata: TableMetadata, timeColumn: String, timeRange: TimeRange, connection: Connection): TableData = {

    def tableDataReader(resultSet: ResultSet, accum: TableData.Rows): TableData.Rows = {
      accum :+ tableMetadata.metadata.map { field =>
        val dataType = tableMetadata.columnMetadataForFieldName(field.name).get._type
        field.name -> getFieldWithDataType(field.name, dataType, resultSet)
      }.toMap
    }

    val startDate = new java.sql.Date(Helper.localDateTimeToMillis(timeRange.start))
    val endDate = new java.sql.Date(Helper.localDateTimeToMillis(timeRange.end))

    val statement = connection.createStatement()

    val query = s"select * " +
      s"from ${Config.PostgreSqlServer.schema}.${tableMetadata.table} " +
      s"where $timeColumn between '$startDate' and '$endDate'"

    val resultSet = statement.executeQuery(query)

    TableData(tableMetadata, iterateOverResultSet(resultSet, TableData.tuples(), tableDataReader))

  }

  def customTimeIntervals: List[TimeRange] = {

    val years = 2012 to LocalDate.now().getYear
    val months = List(Month.JANUARY, Month.FEBRUARY, Month.MARCH, Month.APRIL, Month.MAY, Month.JUNE, Month.JULY, Month.AUGUST, Month.SEPTEMBER, Month.OCTOBER, Month.NOVEMBER, Month.DECEMBER)


    def monthRange(year: Int, month: Month): DateRange = {
      val start = LocalDate.of(year, month, 1)
      val end = LocalDate.of(
        if (month != Month.DECEMBER) year else year + 1,
        month.plus(1),
        1
      )
      DateRange(start, end)
    }

    def timeRanges(year: Int, month: Month, minimumSlicesPerMonth: Int): List[TimeRange] = {

      val monthBounds = monthRange(year, month)
      val periodInDays = ChronoUnit.DAYS.between(monthBounds.start, monthBounds.end).toInt
      val slices = (0 until periodInDays by (periodInDays / minimumSlicesPerMonth)) :+ periodInDays

      println(year)
      println(month)
      println(slices)

      slices.
        sliding(size = 2, step = 1).map { range =>
        TimeRange(
          LocalDateTime.of(year, month, range.head + 1, 0, 0),
          LocalDateTime.of(year, month, range.last, 0, 0)
        )
      }.toList
    }

    def timeRangesDaysInMonth(year: Int, month: Month): List[TimeRange] = {
      val monthBounds = monthRange(year, month)
      val periodInDays = ChronoUnit.DAYS.between(monthBounds.start, monthBounds.end).toInt

      timeRanges(year, month, periodInDays)
    }


    (for {
      y <- years
      m <- months
    } yield {
      timeRanges(y, m, 10)
    }).toList.flatten

  }

}
