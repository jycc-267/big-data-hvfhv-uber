package hvfhv.speed

import hvfhv.common.CassandraSchema.Speed
import hvfhv.common.Metrics._
import hvfhv.common.TripFixtures._
import hvfhv.common._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable

class SpeedWriterSpec extends AnyFunSuite with SparkTestSession {

  /** Behaves like the Cassandra table: rows are upserted by primary key. */
  private class UpsertingSink extends SpeedSink {
    private val keyColumns = TripTransforms.RouteColumns ++ Seq(Speed.RequestDate, Speed.QueryId, Speed.BatchId)
    val rows: mutable.Map[Seq[Any], Row] = mutable.Map.empty

    override def upsert(df: DataFrame): Unit =
      df.collect().foreach(r => rows(keyColumns.map(c => r.get(r.fieldIndex(c)))) = r)

    /** What a reader sums for one route and request date. */
    def total(route: Route, date: String, column: String): Long =
      rows.values.filter { r =>
        r.getAs[String](TripTransforms.Carrier) == route.carrier &&
          r.getAs[String](TripTransforms.PickupZone) == route.pickup &&
          r.getAs[String](TripTransforms.DropoffZone) == route.dropoff &&
          r.getAs[Int](TripTransforms.HourInDay) == route.hour &&
          r.getAs[String](Speed.RequestDate) == date
      }.map(_.getAs[Long](column)).sum
  }

  private def microBatch(trips: Map[String, String]*): DataFrame = TripSchema.normalize(rawJson(spark, trips))

  private val jfkMidtown8 = Route("Uber", "JFK Airport", "Midtown Center", 8)

  test("a micro-batch becomes per-route, per-request-date rows computed like the batch layer") {
    val sink = new UpsertingSink
    new SpeedWriter(ZoneNames, sink).process(microBatch(Base, trip("tolls" -> null), trip("dolocationid" -> "7")), batchId = 0)

    assert(sink.rows.size == 1, "the unknown dropoff zone is dropped, not stored under a null key")
    assert(sink.total(jfkMidtown8, "20220102", Tolls.total) == 655L)
    assert(sink.total(jfkMidtown8, "20220102", Tolls.count) == 1L)
    assert(sink.total(jfkMidtown8, "20220102", WaitTime.total) == 600L) // seconds
    assert(sink.total(jfkMidtown8, "20220102", TripCount) == 2L)
  }

  test("every trip in the micro-batch is applied (regression: only ~11 per batch were)") {
    val sink = new UpsertingSink
    new SpeedWriter(ZoneNames, sink).process(microBatch(Seq.fill(250)(Base): _*), batchId = 0)
    assert(sink.total(jfkMidtown8, "20220102", TripTime.count) == 250L)
  }

  test("a replayed micro-batch rewrites the same rows instead of counting twice") {
    val sink = new UpsertingSink
    val writer = new SpeedWriter(ZoneNames, sink)

    writer.process(microBatch(Base), batchId = 0)
    writer.process(microBatch(Base, Base), batchId = 1)
    writer.process(microBatch(Base, Base), batchId = 1) // restart replays batch 1
    writer.process(microBatch(Base), batchId = 2)

    assert(sink.rows.size == 3)
    assert(sink.total(jfkMidtown8, "20220102", TripCount) == 4L)
  }
}
