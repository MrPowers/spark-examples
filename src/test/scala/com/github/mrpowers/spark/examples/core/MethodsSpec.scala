package com.github.mrpowers.spark.examples.core

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest._
import org.apache.spark.sql.functions._

class MethodsSpec extends FunSpec with ShouldMatchers with DataFrameSuiteBase {

  import spark.implicits._

  describe("#agg") {

    it("HACK - don't know what this does") {

      val sourceDf = Seq(
        ("jose", "blue"),
        ("li", "blue"),
        ("luisa", "red")
      ).toDF("name", "color")

      val df = sourceDf.agg(max(col("color")))

      // HACK - this isn't getting me what I want
      // might need to ask Milin for help

    }

  }

  describe("#alias") {

    it("aliases a DataFrame") {

      val sourceDf = Seq(
        ("jose"),
        ("li"),
        ("luisa")
      ).toDF("name")

      val actualDf = sourceDf.select(col("name").alias("student"))

      val expectedDf = Seq(
        ("jose"),
        ("li"),
        ("luisa")
      ).toDF("student")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

}
