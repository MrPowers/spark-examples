package com.github.mrpowers.spark.examples

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.functions._
import org.scalatest.FunSpec

class RlikeSpec
    extends FunSpec
    with DataFrameSuiteBase {

  import spark.implicits._

  it("finds when a string contains a substring") {

    val keywords = Array(
      "was  found",
      "WAS detected"
    )

    val cleanKeywords = keywords.map { (s: String) =>
      s.toLowerCase().replaceAll("\\s", "")
    }

    val df = List(
      ("A THING was FOUND"),
      ("It was detected"),
      ("I like movies")
    ).toDF("comments")
      .withColumn(
        "clean_comments",
        lower(regexp_replace(col("comments"), "\\s+", ""))
      )
      .withColumn(
        "contains_keyword",
        col("clean_comments").rlike(cleanKeywords.mkString("|"))
      )

    df.show(truncate = false)

  }

}
