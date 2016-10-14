package lasalle.dataportpecanstreet.extract.table


object TableData {
  type Register = Map[String, String]
  type Registers = List[Register]

  def register() = Map[String, String]()
  def registers() = List(register())

}

case class TableData(tableMetadata: TableMetadata, tableData: TableData.Registers)