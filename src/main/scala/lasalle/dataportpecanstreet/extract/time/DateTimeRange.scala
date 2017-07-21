package lasalle.dataportpecanstreet.extract.time

import java.time.{Duration, LocalDateTime, ZoneOffset}

import scala.annotation.tailrec

case class DateTimeRange(start: LocalDateTime, end: LocalDateTime) {

  case class LongRange(start: Long, end: Long)

  def slice(duration: Duration): List[DateTimeRange] = {
    val epochDuration = if (duration.getNano > 0) duration.getSeconds + 1 else duration.getSeconds

    @tailrec
    def _slice(start: Long, end: Long, acc: List[LongRange]): List[LongRange] = {
      val shift = start + epochDuration
      shift.compareTo(end) match {
        case x if x > 0 =>
          if (acc.headOption.nonEmpty && acc.head.end.compareTo(end) < 0)
            LongRange(acc.head.end, end) :: acc
          else
            acc
        case x if x == 0 => LongRange(start, shift) :: acc
        case x if x < 0 => _slice(shift, end, LongRange(start + 1, shift) :: acc)
      }
    }

    _slice(start.toEpochSecond(ZoneOffset.UTC), end.toEpochSecond(ZoneOffset.UTC), List[LongRange]())
      .map { range =>
        DateTimeRange(
          LocalDateTime.ofEpochSecond(range.start, 0, ZoneOffset.UTC),
          LocalDateTime.ofEpochSecond(range.end, 0, ZoneOffset.UTC)
        )
      }

  }

}

/*object Main {
  def main(args: Array[String]): Unit = {
    val now = LocalDateTime.now()
    val future = now.plusMonths(10)
    println(now)
    println(future)
    println(DateTimeRange(now, future).slice(Period.ofMonths(1)))
  }
}*/



