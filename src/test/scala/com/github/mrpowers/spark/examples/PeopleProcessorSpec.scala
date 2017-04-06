package com.github.mrpowers.spark.examples

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.FunSpec

class PeopleProcessorSpec extends FunSpec with DataFrameSuiteBase {

  import spark.implicits._

  describe("run") {

    it("adds age and senior_citizen columns to a DataFrame") {

      val sourceDF = List(
        ("phil", 1975),
        ("janet", 2016),
        ("astor", 1935),
        ("li", 1985),
        ("mike", 1940)
      ).toDF("name", "birth_year")

      val actualDF = PeopleProcessor.run(sourceDF)

      actualDF.show()

    }

  }

}
