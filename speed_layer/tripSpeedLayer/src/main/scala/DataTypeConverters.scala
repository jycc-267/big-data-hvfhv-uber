import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.Try

object DataTypeConverters {
  // Function to convert Option[String] to Option[Double]
  def stringToDouble(s: Option[String]): Option[Double] = {
    s.flatMap(str => Try(str.toDouble).toOption)
  }

  // Function to convert Option[String] to Option[Timestamp]
  def stringToTimestamp(s: Option[String]): Option[Timestamp] = {
    s.flatMap(str => Try {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
      val localDateTime = LocalDateTime.parse(str, formatter)
      Timestamp.valueOf(localDateTime)
    }.toOption)
  }

  // Function to convert Option[String] to Option[Long]
  def stringToLong(s: Option[String]): Option[Long] = {
    s.flatMap(str => Try(str.toLong).toOption)
  }

  // Safe method to get timestamp in milliseconds
  def getTimestampMillis(ts: Option[Timestamp]): Long = {
    ts.map(_.getTime).getOrElse(0L)
  }

  // Safe method to get hour from timestamp
  def getHourFromTimestamp(ts: Option[Timestamp]): String = {
    ts.map(_.toLocalDateTime.getHour.toString).getOrElse("0")
  }

  // Safe method to get double value with default
  def getDoubleOrDefault(d: Option[Double], default: Double = 0.0): Double = {
    d.getOrElse(default)
  }
}

