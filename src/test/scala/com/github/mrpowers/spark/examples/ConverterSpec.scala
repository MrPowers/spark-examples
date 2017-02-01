package com.github.mrpowers.spark.examples

import org.scalatest._
import com.holdenkarau.spark.testing.DataFrameSuiteBase

class ConverterSpec extends FunSpec with ShouldMatchers with DataFrameSuiteBase {

  import spark.implicits._

  describe(".snakecaseify") {

    it("downcases uppercase letters") {
      Converter.snakecaseify("HeLlO") should equal("hello")
    }

    it("converts spaces to underscores") {
      Converter.snakecaseify("Hi There") should equal("hi_there")
    }

  }

  describe(".underscoreColumnNames") {

    it("snake_cases the column names of a DataFrame") {

      val sourceDf = Seq(
        ("funny", "joke")
      ).toDF("A b C", "de F")

      val actualDf = Converter.snakeCaseColumns(sourceDf)

      val expectedDf = Seq(
        ("funny", "joke")
      ).toDF("a_b_c", "de_f")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

}

