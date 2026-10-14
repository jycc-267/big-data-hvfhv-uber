package hvfhv.batch

import java.nio.file.Files
import java.util.UUID

import hvfhv.common.Metrics._
import hvfhv.common.TripFixtures._
import hvfhv.common._
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalatest.funsuite.AnyFunSuite

class BatchLayerSpec extends AnyFunSuite with SparkTestSession {

  private val jfkMidtown8 = Route("Uber", "JFK Airport", "Midtown Center", 8)

  private def trips(rows: Map[String, String]*): DataFrame = TripSchema.normalize(rawJson(spark, rows))

  private def onDay(day: String, extra: (String, String)*): Map[String, String] =
    trip(Seq("request_datetime" -> s"${day}T08:00:00.000", "pickup_datetime" -> s"${day}T08:05:00.000") ++ extra: _*)

  /** Empty master tables shaped like deploy/hive/master_tables.hql, in a fresh database. */
  private def masterTables(): MasterTables = {
    val db = "t" + UUID.randomUUID().toString.replace("-", "").take(12)
    spark.sql(s"CREATE DATABASE $db LOCATION '${Files.createTempDirectory(db)}'")
    val tables = MasterTables(db)
    Seq(tables.bulk, tables.stream).foreach { t =>
      spark.sql(s"CREATE TABLE $t (${TripSchema.Canonical.toDDL}) USING orc PARTITIONED BY (${TripSchema.RequestDate})")
    }
    tables
  }

  /** Writes trips the way the speed layer's archive query does: ORC partitioned by date. */
  private def landing(rows: Map[String, String]*): String = {
    val dir = Files.createTempDirectory("landing").toString + "/events"
    trips(rows: _*).write.partitionBy(TripSchema.RequestDate).orc(dir)
    dir
  }

  private def staging(): String = Files.createTempDirectory("staging").toString

  private def countsByDate(df: DataFrame): Map[String, Long] =
    df.groupBy(TripSchema.RequestDate).count().collect().map(r => r.getString(0) -> r.getLong(1)).toMap

  test("watermark: a bulk load cut at midnight is complete through its last day") {
    assert(Watermark.choose(Some("20220101"), None).contains("20220102"))
  }

  test("watermark: the newest streamed day may still be filling, earlier days are complete") {
    assert(Watermark.choose(Some("20220101"), Some("20220104")).contains("20220104"))
    // Streamed trips older than the bulk load must not move the watermark back.
    assert(Watermark.choose(Some("20220110"), Some("20220104")).contains("20220111"))
    assert(Watermark.choose(None, None).isEmpty)
    assert(Watermark.nextDay("20221231") == "20230101")
  }

  test("re-ingesting the bulk load replaces every partition, including dates no longer in it") {
    val tables = masterTables()
    MasterDataset.replaceBulk(spark, trips(onDay("2022-01-01"), onDay("2022-01-02")), tables)
    MasterDataset.replaceBulk(spark, trips(onDay("2022-01-03")), tables)
    assert(countsByDate(MasterDataset.read(spark, tables).bulk) == Map("20220103" -> 1L))
  }

  test("absorbing the landing zone copies new trips, drops replayed duplicates and is idempotent") {
    val tables = masterTables()
    // Already in Hive: one trip on Jan 2 and an unrelated partition on Jan 5.
    trips(onDay("2022-01-02"), onDay("2022-01-05")).write.mode(SaveMode.Append).insertInto(tables.stream)

    val replayed = onDay("2022-01-03", "tips" -> "1.00")
    val landingDir = landing(onDay("2022-01-02"), onDay("2022-01-02", "tips" -> "9.00"), replayed, replayed)

    assert(MasterDataset.absorbStream(spark, landingDir, staging(), tables) == 2)
    val once = countsByDate(MasterDataset.read(spark, tables).stream)
    assert(once == Map("20220102" -> 2L, "20220103" -> 1L, "20220105" -> 1L))

    MasterDataset.absorbStream(spark, landingDir, staging(), tables)
    assert(countsByDate(MasterDataset.read(spark, tables).stream) == once)
  }

  test("absorbing an empty or missing landing zone is a no-op") {
    val tables = masterTables()
    assert(MasterDataset.absorbStream(spark, "/nonexistent/landing", staging(), tables) == 0)
  }

  test("the master dataset unions bulk and streamed trips without double counting") {
    val tables = masterTables()
    MasterDataset.replaceBulk(spark, trips(onDay("2022-01-01")), tables)
    MasterDataset.absorbStream(spark, landing(onDay("2022-01-02"), onDay("2022-01-02")), staging(), tables)
    val master = MasterDataset.read(spark, tables)

    assert(master.trips.count() == 2)
    assert(MasterDataset.maxRequestDate(master.bulk).contains("20220101"))
    assert(MasterDataset.maxRequestDate(master.stream).contains("20220102"))
  }

  test("batch view only covers request dates strictly before the watermark") {
    val view = statsByRoute(BuildBatchView.compute(trips(Base, onDay("2022-01-03")), ZoneNames, "20220103"))
    assert(view.keySet == Set(jfkMidtown8))
    assert(view(jfkMidtown8)(TripTime.count) == 1L)
  }

  /** Rows for the JFK -> Midtown, 8am route: (request date, tolls total, tolls count), other metrics 0. */
  private def routeRows(rows: (String, Long, Long)*): DataFrame = {
    import spark.implicits._
    val metrics = Metrics.Columns.map {
      case c if c == Tolls.total => col("total").as(c)
      case c if c == Tolls.count => col("count").as(c)
      case c => lit(0L).as(c)
    }
    val route = Seq(
      lit("Uber").as(TripTransforms.Carrier), lit("JFK Airport").as(TripTransforms.PickupZone),
      lit("Midtown Center").as(TripTransforms.DropoffZone), lit(8).as(TripTransforms.HourInDay),
      col("date").as(TripSchema.RequestDate))
    rows.toDF("date", "total", "count").select(route ++ metrics: _*)
  }

  test("served view = batch row + speed rows at or after the watermark; older speed rows are ignored") {
    val batch = routeRows(("20220101", 100L, 2L)).drop(TripSchema.RequestDate)
    val speed = routeRows(
      ("20220102", 999L, 9L), // superseded by the batch view
      ("20220103", 50L, 1L),
      ("20220103", 20L, 1L), // a second micro-batch on the same day
      ("20220104", 25L, 1L))

    val served = statsByRoute(Reconciliation.served(batch, speed, "20220103"))
    assert(served(jfkMidtown8)(Tolls.total) == 195L)
    assert(served(jfkMidtown8)(Tolls.count) == 5L)
  }

  test("reconciliation reports exactly the routes whose served totals differ") {
    val expected = routeRows(("x", 175L, 4L)).drop(TripSchema.RequestDate)
    assert(Reconciliation.mismatches(expected, expected).count() == 0)
    assert(Reconciliation.mismatches(expected, routeRows(("x", 200L, 5L)).drop(TripSchema.RequestDate)).count() == 1)
    assert(Reconciliation.mismatches(expected, expected.where(lit(false))).count() == 1, "missing on the served side")
  }

  test("end to end in Spark: batch(< W) + speed(>= W) equals a full recompute") {
    val day2 = Seq(Base, trip("tolls" -> null))
    val day3 = Seq(onDay("2022-01-03"), onDay("2022-01-03", "dolocationid" -> "230"))
    val all = trips(day2 ++ day3: _*)

    val batch = BuildBatchView.compute(all, ZoneNames, "20220103")
    val speed = TripTransforms.aggregate(TripTransforms.enrich(trips(day3: _*), ZoneNames), byRequestDate = true)
    val expected = TripTransforms.aggregate(TripTransforms.enrich(all, ZoneNames), byRequestDate = false)

    assert(Reconciliation.mismatches(expected, Reconciliation.served(batch, speed, "20220103")).count() == 0)
  }
}
