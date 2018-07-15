package lasalle.dataportpecanstreet.extract.time

import java.time.{Duration, LocalDateTime, ZoneOffset}

import scala.annotation.tailrec

case class DateTimeRange(start: LocalDateTime, end: LocalDateTime) {

  case class LongRange(start: Long, end: Long)

  private def getTotalSecondsCeiled(duration: Duration) = Math.ceil(duration.getNano)

  def slice(duration: Duration): List[DateTimeRange] = {
    val epochDuration = duration.getSeconds + Math.ceil(duration.getNano).toLong // ceil to seconds

    @tailrec
    def _slice(start: Long, end: Long, acc: List[LongRange]): List[LongRange] = {
      val shift = start + epochDuration
      shift.compareTo(end) match {
        case x if x > 0 =>
          if (acc.headOption.nonEmpty && acc.head.end.compareTo(end) < 0)
            LongRange(acc.head.end + 1, end) :: acc
          else
            acc
        case x if x == 0 => LongRange(start + 1, shift) :: acc
        case x if x < 0 => _slice(shift, end, LongRange(start + 1, shift) :: acc)
      }
    }

    _slice(start.toEpochSecond(ZoneOffset.UTC) - 1 , end.toEpochSecond(ZoneOffset.UTC), List[LongRange]())
      .map { range =>
        DateTimeRange(
          LocalDateTime.ofEpochSecond(range.start, 0, ZoneOffset.UTC),
          LocalDateTime.ofEpochSecond(range.end, 0, ZoneOffset.UTC)
        )
      }

  }

}




