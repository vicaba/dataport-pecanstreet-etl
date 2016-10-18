package lasalle.dataportpecanstreet.extract.table

sealed trait  DataType

object DataType {

  case class Undefined(value: String) extends DataType

  /**
    * Should map to Integer
    */
  case object Integer extends DataType

  /**
    * Should map to Double
    */
  case object Decimal extends DataType

  /**
    * Should map to String
    */
  case object String extends DataType

  /**
    * Should map to java Calendar
    */
  case object Timestamp extends DataType
}