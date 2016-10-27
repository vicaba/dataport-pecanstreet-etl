package lasalle.dataportpecanstreet.extract.time

import java.time.temporal.{ChronoUnit, TemporalUnit}
import java.time.{Duration, LocalDateTime, Period}
import java.util.Calendar

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

case class TimeRange(start: LocalDateTime, end: LocalDateTime) {

  def slice(period: Period): List[TimeRange] = {

    @tailrec
    def _slice(start: LocalDateTime, end: LocalDateTime, slices: List[TimeRange]): List[TimeRange] = {
      val shift = start.plus(period)
      shift.compareTo(end) match {
        case x if x > 0 => slices
        case x if x == 0 => TimeRange(start, shift) :: slices
        case x if x < 0 => _slice(shift, end, TimeRange(start, shift) :: slices)
      }
    }

    val slices = _slice(this.start, this.end, List[TimeRange]())

    if (slices.headOption.nonEmpty && slices.head.end.compareTo(this.end) < 0)
      TimeRange(slices.head.end, this.end) :: slices
    else
      slices
  }

}

object Main {
  def main(args: Array[String]): Unit = {
    val now = LocalDateTime.now()
    val future = now.plusMonths(10)
    println(now)
    println(future)
    println(TimeRange(now, future).slice(Period.ofMonths(1)))
  }
}



