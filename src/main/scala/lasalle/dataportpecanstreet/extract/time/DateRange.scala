package lasalle.dataportpecanstreet.extract.time

import java.time.{LocalDate, Period}

import scala.annotation.tailrec

case class DateRange(start: LocalDate, end: LocalDate) {

  def slice(period: Period): List[DateRange] = {

    @tailrec
    def _slice(start: LocalDate, end: LocalDate, slices: List[DateRange]): List[DateRange] = {
      val shift = start.plus(period)
      shift.compareTo(end) match {
        case x if x > 0 => slices
        case x if x == 0 => DateRange(start, shift) :: slices
        case x if x < 0 => _slice(shift, end, DateRange(start, shift) :: slices)
      }
    }

    val slices = _slice(this.start, this.end, List[DateRange]())

    if (slices.headOption.nonEmpty && slices.head.end.compareTo(this.end) < 0)
      DateRange(slices.head.end, this.end) :: slices
    else
      slices
  }

}
/*

object Main {
  def main(args: Array[String]): Unit = {
    val now = LocalDate.now()
    val future = now.plusMonths(10)
    println(now)
    println(future)
    println(DateRange(now, future).slice(Period.ofMonths(1)))
  }
}*/
