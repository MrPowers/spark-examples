package com.github.mrpowers.spark.examples.basic

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest._

class ConverterSpec extends FunSpec with DataFrameSuiteBase {

  import spark.implicits._

  describe(".snakecaseify") {

    it("downcases uppercase letters") {
      assert(Converter.snakecaseify("HeLlO")  === "hello")
    }

    it("converts spaces to underscores") {
      assert(Converter.snakecaseify("Hi There") === "hi_there")
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

