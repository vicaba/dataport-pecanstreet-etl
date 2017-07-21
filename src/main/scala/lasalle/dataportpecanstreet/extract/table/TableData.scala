package lasalle.dataportpecanstreet.extract.table


object TableData {
  type Value = Option[Any]
  type Row = Map[String, Value]
  type Rows = List[Row]

  def emptyTuple() = Map[String, Value]()
  def emptyTuples() = List[Row]()

}

case class TableData(tableMetadata: TableMetadata, tableData: TableData.Rows)