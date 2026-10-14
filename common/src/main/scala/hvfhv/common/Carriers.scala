package hvfhv.common

/** Single source of truth for HVFHS license number -> carrier brand (TLC data dictionary). */
object Carriers {

  val ByLicense: Map[String, String] = Map(
    "HV0002" -> "Juno",
    "HV0003" -> "Uber",
    "HV0004" -> "Via",
    "HV0005" -> "Lyft")

  val Names: Seq[String] = ByLicense.values.toSeq.sorted
}
