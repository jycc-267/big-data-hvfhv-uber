package hvfhv.common

/** Minimal `--name value` parser; everything else comes from [[AppConfig]]. */
object CliArgs {

  def parse(args: Array[String], allowed: Set[String]): Map[String, String] = {
    require(args.length % 2 == 0, s"expected --name value pairs, got: ${args.mkString(" ")}")
    args.grouped(2).map { case Array(name, value) =>
      require(name.startsWith("--") && allowed(name.drop(2)),
        s"unknown option $name; allowed: ${allowed.map("--" + _).mkString(", ")}")
      name.drop(2) -> value
    }.toMap
  }
}
