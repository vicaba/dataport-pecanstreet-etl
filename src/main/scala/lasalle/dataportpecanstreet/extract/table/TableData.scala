package lasalle.dataportpecanstreet.extract.table


object TableData {
  type Tuple = Map[String, Any]
  type Tuples = List[Tuple]

  def tuple() = Map[String, Any]()
  def tuples() = List[Tuple]()

}

case class TableData(tableMetadata: TableMetadata, tableData: TableData.Tuples)