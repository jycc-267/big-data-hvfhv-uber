package hvfhv.common

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}

/**
 * Source fields of the NYC TLC "High Volume FHV Trip Records" dataset and the one place
 * they are parsed into typed columns. Both the historical CSV load and the Kafka JSON
 * stream go through [[normalize]], so the two layers can never parse a field differently.
 */
object TripSchema {

  val RequestDate: String = "request_date"
  val RequestDateFormat: String = "yyyyMMdd"

  private val TimestampFields = Seq("request_datetime", "on_scene_datetime", "pickup_datetime", "dropoff_datetime")
  private val MoneyFields =
    Seq("base_passenger_fare", "tolls", "bcf", "sales_tax", "congestion_surcharge", "airport_fee", "tips", "driver_pay")

  /** Socrata emits ISO timestamps; the portal's bulk "Export" uses the US format. Accept both. */
  private val TimestampFormats = Seq("yyyy-MM-dd'T'HH:mm:ss[.SSS]", "MM/dd/yyyy hh:mm:ss a")

  /** Exact cents; doubles would drift when summed across millions of trips. */
  val MoneyType: DecimalType = DecimalType(10, 2)

  private val Fields: Seq[(String, DataType)] = Seq(
    "hvfhs_license_num" -> StringType,
    "dispatching_base_num" -> StringType,
    "originating_base_num" -> StringType,
    "request_datetime" -> TimestampType,
    "on_scene_datetime" -> TimestampType,
    "pickup_datetime" -> TimestampType,
    "dropoff_datetime" -> TimestampType,
    "pulocationid" -> IntegerType,
    "dolocationid" -> IntegerType,
    "trip_miles" -> DoubleType,
    "trip_time" -> LongType,
    "base_passenger_fare" -> MoneyType,
    "tolls" -> MoneyType,
    "bcf" -> MoneyType,
    "sales_tax" -> MoneyType,
    "congestion_surcharge" -> MoneyType,
    "airport_fee" -> MoneyType,
    "tips" -> MoneyType,
    "driver_pay" -> MoneyType,
    "shared_request_flag" -> StringType,
    "shared_match_flag" -> StringType,
    "access_a_ride_flag" -> StringType,
    "wav_request_flag" -> StringType,
    "wav_match_flag" -> StringType)

  val FieldNames: Seq[String] = Fields.map(_._1)

  /** Everything as strings: the shape of the CSV files and of the JSON messages on Kafka. */
  val Raw: StructType = StructType(FieldNames.map(StructField(_, StringType)))

  /** Typed master-dataset record. */
  val Canonical: StructType = StructType(
    Fields.map { case (name, dataType) => StructField(name, dataType) } :+ StructField(RequestDate, StringType))

  /**
   * Parses string-typed source columns into [[Canonical]]. Columns missing from the input
   * (Socrata omits null fields from JSON) become nulls; unparseable values become nulls.
   * Requires `spark.sql.legacy.timeParserPolicy=CORRECTED` (see [[SparkSessions]]).
   */
  def normalize(raw: DataFrame): DataFrame = {
    val present = raw.columns.map(_.toLowerCase).toSet
    val typed = Fields.map { case (name, dataType) =>
      val source = if (present(name)) blankToNull(col(name)) else lit(null).cast(StringType)
      parse(source, name, dataType).as(name)
    }
    raw
      .select(typed: _*)
      .withColumn(RequestDate, date_format(col("request_datetime"), RequestDateFormat))
  }

  private def parse(c: Column, name: String, dataType: DataType): Column =
    if (TimestampFields.contains(name)) coalesce(TimestampFormats.map(to_timestamp(c, _)): _*)
    else if (MoneyFields.contains(name)) c.cast(MoneyType)
    else c.cast(dataType)

  private def blankToNull(c: Column): Column = {
    val trimmed = trim(c)
    when(trimmed === "", lit(null).cast(StringType)).otherwise(trimmed)
  }
}
