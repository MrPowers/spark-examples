package com.github.mrpowers.spark.examples.basic

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, IntegerType, StructField, StructType}
import org.scalatest._

class NumberFunSpec extends FunSpec with ShouldMatchers with DataFrameSuiteBase {

  describe(".isEven") {

    it("returns true for even numbers") {
      NumberFun.isEven(4) should equal(true)
    }

    it("returns false for odd numbers") {
      NumberFun.isEven(3) should equal(false)
    }

    it("returns false for null values") {
      NumberFun.isEven(null) should equal(false)
    }

    it("appends an is_even column to a Dataframe") {

      val sourceSchema = List(
        StructField("number", IntegerType, true)
      )

      val sourceData = List(
        Row(1),
        Row(8),
        Row(12),
        Row(null)
      )

      val sourceDf = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )

      val actualDf = sourceDf.withColumn("is_even", NumberFun.isEvenUdf(col("number")))

      val expectedSchema = List(
        StructField("number", IntegerType, true),
        StructField("is_even", BooleanType, true)
      )

      val expectedData = List(
        Row(1, false),
        Row(8, true),
        Row(12, true),
        Row(null, false)
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe(".isEvenOption") {

    it("returns true for even numbers") {
      NumberFun.isEvenOption(4) should equal(Some(true))
    }

    it("returns false for odd numbers") {
      NumberFun.isEvenOption(3) should equal(Some(false))
    }

    it("returns false for null values") {
      NumberFun.isEvenOption(null) should equal(Some(false))
    }

    it("appends an is_even column to a Dataframe") {

      val sourceSchema = List(
        StructField("number", IntegerType, true)
      )

      val sourceData = List(
        Row(1),
        Row(8),
        Row(12),
        Row(null)
      )

      val sourceDf = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )

      val actualDf = sourceDf.withColumn("is_even", NumberFun.isEvenOptionUdf(col("number")))

      val expectedSchema = List(
        StructField("number", IntegerType, true),
        StructField("is_even", BooleanType, true)
      )

      val expectedData = List(
        Row(1, false),
        Row(8, true),
        Row(12, true),
        Row(null, false)
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

}

