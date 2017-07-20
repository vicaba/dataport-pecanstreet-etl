package lasalle.dataportpecanstreet.extract.time

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}

object Helper {

  def sqlTimestampToLocalTimeDate(t: Timestamp): LocalDateTime = t.toLocalDateTime

  def localDateTimeToMillis(t: LocalDateTime): Long = t.toInstant(ZoneOffset.ofTotalSeconds(0)).toEpochMilli

}
