package lasalle.dataportpecanstreet.extract.table

sealed trait DataType

object DataType {
  case class Undefined(value: String) extends DataType
  case object Integer extends DataType
  case object Decimal extends DataType
  case object String extends DataType
}