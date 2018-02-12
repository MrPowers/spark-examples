package com.github.mrpowers.spark.examples

object Config {

  val test: Map[String, String] = {
    Map(
      "stateMappingsPath" -> new java.io.File(s"./src/test/resources/state_mappings.csv").getCanonicalPath,
      "weirdMatchesPath" -> new java.io.File(s"./src/test/resources/random_matches.csv").getCanonicalPath
    )
  }

  val production: Map[String, String] = {
    Map(
      "stateMappingsPath" -> "s3a://some-fake-bucket/state_mappings.csv"
    )
  }

  var environment = sys.env.getOrElse("PROJECT_ENV", "production")

  def get(key: String): String = {
    if (environment == "test") {
      test(key)
    } else {
      production(key)
    }
  }

}
