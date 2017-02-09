package com.github.mrpowers.spark.examples.core

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class MethodsSpec extends FunSpec with ShouldMatchers with DataFrameSuiteBase {

  import spark.implicits._

  describe("#methodsMissing") {

    it("lists out the methods not included in this file") {
      // cache - hard to demonstrate
      // checkpoint - don't know what it means, think it's for streaming
      // classTag - not sure what this is for, maybe Java?
      // coalesce - not well suited for test file
      // col - shown as part of other examples
      // collectAsList - seems like a Java thing
      // createGlobalTempView
      // createOrReplaceTempView
      // createTempView
      // cube
    }

  }

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

  describe("#as") {

    it("does the same thing as alias") {

      val sourceDf = Seq(
        ("jose"),
        ("li"),
        ("luisa")
      ).toDF("name")

      val actualDf = sourceDf.select(col("name").as("student"))

      val expectedDf = Seq(
        ("jose"),
        ("li"),
        ("luisa")
      ).toDF("student")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#collect") {

    it("returns an array of Rows in the DataFrame") {

      val row1 = Row("cat")
      val row2 = Row("dog")

      val sourceData = List(
        row1,
        row2
      )

      val sourceSchema = List(
        StructField("animal", StringType, true)
      )

      val sourceDf = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )

      val s = sourceDf.collect()

      s should equal(Array(row1, row2))

    }

  }

  describe("#columns") {

    it("returns all the column names as an array") {

      val sourceDf = Seq(
        ("jets", "football"),
        ("nacional", "soccer")
      ).toDF("team", "sport")

      val expected = Array("team", "sport")

      sourceDf.columns should equal(expected)

    }

  }

  describe("#count") {

    it("returns a count of all the rows in a DataFrame") {

      val sourceDf = Seq(
        ("jets"),
        ("barcelona")
      ).toDF("team")

      sourceDf.count should equal(2)

    }

  }

  describe("#crossJoin") {

    it("returns a count of all the rows in a DataFrame") {

      val letterDf = Seq(
        ("a"),
        ("b")
      ).toDF("letter")

      val numberDf = Seq(
        ("1"),
        ("2")
      ).toDF("number")

      val actualDf = letterDf.crossJoin(numberDf)

      val expectedDf = Seq(
        ("a", "1"),
        ("a", "2"),
        ("b", "1"),
        ("b", "2")
      ).toDF("letter", "number")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#dropDuplicates") {

    it("drops the duplicate rows from a DataFrame") {

      val numbersDf = Seq(
        (1, 2),
        (8, 8),
        (1, 2),
        (5, 6),
        (8, 8)
      ).toDF("num1", "num2")

      val actualDf = numbersDf.dropDuplicates()

      val expectedDf = Seq(
        (1, 2),
        (5, 6),
        (8, 8)
      ).toDF("num1", "num2")

      assertDataFrameEquals(actualDf, expectedDf)

    }

    it("drops duplicate rows based on certain columns") {

      val numbersDf = Seq(
        (1, 2, 100),
        (8, 8, 100),
        (1, 2, 200),
        (5, 6, 7),
        (8, 8, 50)
      ).toDF("num1", "num2", "num3")

      val actualDf = numbersDf.dropDuplicates("num1", "num2")

      val sourceData = List(
        Row(1, 2, 100),
        Row(5, 6, 7),
        Row(8, 8, 100)
      )

      val sourceSchema = List(
        StructField("num1", IntegerType, false),
        StructField("num2", IntegerType, false),
        StructField("num3", IntegerType, true)
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

}
