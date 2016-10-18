package lasalle.dataportpecanstreet.extract.table


object TableData {
  type Value = Option[Any]
  type Row = Map[String, Value]
  type Rows = List[Row]

  def tuple() = Map[String, Value]()
  def tuples() = List[Row]()

}

case class TableData(tableMetadata: TableMetadata, tableData: TableData.Rows)