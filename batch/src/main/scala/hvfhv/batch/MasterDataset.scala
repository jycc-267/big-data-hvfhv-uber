package hvfhv.batch

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import hvfhv.common.{MasterTables, TripSchema}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{col, lit, max}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/**
 * The master dataset, read from its Hive tables: the historical bulk load plus every
 * trip the speed layer delivered. Both hold [[TripSchema.Canonical]] records.
 */
final case class MasterDataset(bulk: DataFrame, stream: DataFrame) {

  /**
   * All trips, with exact duplicates removed. Duplicates appear when the producer replays
   * a page after a crash; the batch view must count such a trip once.
   */
  def trips: DataFrame = bulk.unionByName(stream).dropDuplicates(TripSchema.FieldNames)
}

object MasterDataset {

  private val log = LoggerFactory.getLogger(getClass)
  private val Columns = TripSchema.Canonical.fieldNames.map(col)

  def read(spark: SparkSession, tables: MasterTables): MasterDataset =
    MasterDataset(spark.table(tables.bulk).select(Columns: _*), spark.table(tables.stream).select(Columns: _*))

  /** Replaces the whole bulk table, every request date, with `trips`. */
  def replaceBulk(spark: SparkSession, trips: DataFrame, tables: MasterTables): Unit =
    withOverwriteMode(spark, "static") {
      trips.select(Columns: _*).write.mode(SaveMode.Overwrite).insertInto(tables.bulk)
    }

  /**
   * Copies the speed layer's landing zone into the Hive stream table, so Hive holds every
   * trip. Spark honours the streaming sink's `_spark_metadata` log, so only files from
   * committed micro-batches are copied. For each request date present in the landing
   * zone, the existing Hive partition and the landed trips are merged, de-duplicated and
   * written back; other partitions are untouched. Re-running copies nothing new.
   *
   * @return the number of request dates (partitions) rewritten
   */
  def absorbStream(spark: SparkSession, landingPath: String, stagingPath: String, tables: MasterTables): Int =
    readLanding(spark, landingPath) match {
      case None => 0
      case Some(landed) =>
        val dates = landed.select(TripSchema.RequestDate).distinct().collect().map(r => Option(r.getString(0)))
        if (dates.nonEmpty) {
          val touched = col(TripSchema.RequestDate).isin(dates.flatten: _*) ||
            (lit(dates.contains(None)) && col(TripSchema.RequestDate).isNull)
          val merged = spark.table(tables.stream).select(Columns: _*).where(touched)
            .unionByName(landed)
            .dropDuplicates(TripSchema.FieldNames)

          // Spark refuses to overwrite a table it is reading in the same query, so stage first.
          val staging = s"$stagingPath/absorb"
          merged.write.mode(SaveMode.Overwrite).orc(staging)
          withOverwriteMode(spark, "dynamic") {
            spark.read.schema(TripSchema.Canonical).orc(staging)
              .select(Columns: _*).write.mode(SaveMode.Overwrite).insertInto(tables.stream)
          }
          log.info(s"Absorbed the speed layer's landing zone into ${tables.stream}: ${dates.length} request dates rewritten")
        }
        dates.length
    }

  /**
   * Trips in the landing zone that are already counted by the speed view, whether or not a
   * batch run has absorbed them yet. Only files from committed micro-batches are visible.
   */
  def readLanding(spark: SparkSession, landingPath: String): Option[DataFrame] =
    if (exists(spark, landingPath)) Some(spark.read.schema(TripSchema.Canonical).orc(landingPath).select(Columns: _*))
    else None

  def maxRequestDate(trips: DataFrame): Option[String] =
    Option(trips.agg(max(col(TripSchema.RequestDate))).head().getString(0))

  /** insertInto ignores the per-write option, so the mode is set on the session for the write. */
  private def withOverwriteMode[A](spark: SparkSession, mode: String)(f: => A): A = {
    val key = "spark.sql.sources.partitionOverwriteMode"
    val previous = spark.conf.get(key)
    spark.conf.set(key, mode)
    try f
    finally spark.conf.set(key, previous)
  }

  private def exists(spark: SparkSession, path: String): Boolean = {
    val p = new Path(path)
    p.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(p)
  }
}

/**
 * The batch view covers request dates strictly before the watermark; the speed view serves
 * the watermark date onwards. A date may only fall below the watermark once the master
 * dataset holds all of its trips, otherwise those trips would be served by neither layer.
 */
object Watermark {

  private val Format = DateTimeFormatter.ofPattern(TripSchema.RequestDateFormat)

  /**
   * @param bulkMax   newest request date in the bulk load. The load is cut at a day
   *                  boundary (scripts/download_sample.sh), so that day is complete.
   * @param streamMax newest request date that arrived through the speed layer. Trips
   *                  arrive in request order, so earlier dates are complete; this one may not be.
   */
  def choose(bulkMax: Option[String], streamMax: Option[String]): Option[String] =
    (bulkMax.map(nextDay) ++ streamMax).reduceOption((a, b) => if (a >= b) a else b)

  def nextDay(date: String): String = LocalDate.parse(date, Format).plusDays(1).format(Format)

  def validate(date: String): String = {
    LocalDate.parse(date, Format) // throws on a malformed date
    date
  }
}
