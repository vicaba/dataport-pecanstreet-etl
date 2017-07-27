package lasalle.dataportpecanstreet.extract.table

sealed trait  DataType

object DataType {

  case class Undefined(value: String) extends DataType

  /**
    * Should map to Integer
    */
  case object Integer extends DataType {
    override def toString: String = "Integer"
  }

  /**
    * Should map to Double
    */
  case object Decimal extends DataType {
    override def toString: String = "Decimal"
  }

  /**
    * Should map to String
    */
  case object String extends DataType {
    override def toString: String = "String"
  }

  /**
    * Should map to java LocalDateTime
    */
  case object Timestamp extends DataType {
    override def toString: String = "Timestamp"

  }
}