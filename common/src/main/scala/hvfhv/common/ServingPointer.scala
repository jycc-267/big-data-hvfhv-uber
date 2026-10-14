package hvfhv.common

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.SimpleStatement

/**
 * Which batch view readers should use and the request date it covers up to (exclusive).
 * Readers combine that view with speed rows dated on or after `watermark`.
 */
final case class ServingPointer(batchTable: String, watermark: String, builtAt: String)

object ServingPointer {
  import CassandraSchema.{Pointer, PointerTable}

  def read(session: CqlSession, keyspace: String): Option[ServingPointer] =
    Option(session.execute(SimpleStatement.newInstance(
      s"SELECT ${Pointer.BatchTable}, ${Pointer.Watermark}, ${Pointer.BuiltAt} FROM $keyspace.$PointerTable WHERE name = ?",
      Pointer.Row)).one())
      .map(r => ServingPointer(r.getString(0), r.getString(1), Option(r.getString(2)).getOrElse("")))

  /** One single-partition write, so Cassandra applies table and watermark together. */
  def write(session: CqlSession, keyspace: String, pointer: ServingPointer): Unit =
    session.execute(SimpleStatement.newInstance(
      s"INSERT INTO $keyspace.$PointerTable (name, ${Pointer.BatchTable}, ${Pointer.Watermark}, ${Pointer.BuiltAt}) VALUES (?, ?, ?, ?)",
      Pointer.Row, pointer.batchTable, pointer.watermark, pointer.builtAt))
}
