package hvfhv.common

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.JavaConverters._

object CassandraIO {

  private val Format = "org.apache.spark.sql.cassandra"

  /** Serializable handle; executors open (and pool) their own sessions from it. */
  def connector(spark: SparkSession): CassandraConnector = CassandraConnector(spark.sparkContext.getConf)

  def ensureServingSchema(session: CqlSession, keyspace: String): Unit =
    Seq(
      CassandraSchema.createKeyspace(keyspace),
      CassandraSchema.createSpeed(keyspace),
      CassandraSchema.createPointer(keyspace),
      CassandraSchema.createLookups(keyspace)).foreach(session.execute)

  /** Rows are upserts: writing a row whose primary key exists replaces its columns. */
  def write(rows: DataFrame, keyspace: String, table: String): Unit =
    rows.write.format(Format).options(Map("keyspace" -> keyspace, "table" -> table)).mode(SaveMode.Append).save()

  def read(spark: SparkSession, keyspace: String, table: String): DataFrame =
    spark.read.format(Format).options(Map("keyspace" -> keyspace, "table" -> table)).load()

  def tables(session: CqlSession, keyspace: String): Seq[String] =
    session
      .execute(SimpleStatement.newInstance("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?", keyspace))
      .all().asScala.map(_.getString(0)).toSeq

  def dropTable(session: CqlSession, keyspace: String, table: String): Unit =
    session.execute(s"DROP TABLE IF EXISTS $keyspace.$table")
}
