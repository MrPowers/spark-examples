package com.github.mrpowers.spark.examples.basic

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, IntegerType, StructField, StructType}
import org.scalatest._

class NumberFunSpec extends FunSpec with ShouldMatchers with DataFrameSuiteBase {

  describe(".isEvenSimple") {

    it("returns true for even numbers") {

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

      val actualDf = sourceDf.withColumn(
        "is_even",
        when(
          col("number").isNotNull,
          NumberFun.isEvenSimpleUdf(col("number"))
        ).otherwise(lit(null))
      )

      val expectedSchema = List(
        StructField("number", IntegerType, true),
        StructField("is_even", BooleanType, true)
      )

      val expectedData = List(
        Row(1, false),
        Row(8, true),
        Row(12, true),
        Row(null, null)
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe(".isEvenBad") {

    it("returns true for even numbers") {
      NumberFun.isEvenBad(4) should equal(true)
    }

    it("returns false for odd numbers") {
      NumberFun.isEvenBad(3) should equal(false)
    }

    it("returns false for null values") {
      NumberFun.isEvenBad(null) should equal(false)
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

      val actualDf = sourceDf.withColumn(
        "is_even",
        NumberFun.isEvenBadUdf(col("number"))
      )

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

  describe(".isEvenBetter") {

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

      val actualDf = sourceDf.withColumn(
        "is_even",
        NumberFun.isEvenBetterUdf(col("number"))
      )

      val expectedSchema = List(
        StructField("number", IntegerType, true),
        StructField("is_even", BooleanType, true)
      )

      val expectedData = List(
        Row(1, false),
        Row(8, true),
        Row(12, true),
        Row(null, null)
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
      NumberFun.isEvenOption(null) should equal(None)
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
        Row(null, null)
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe(".isEvenBroke") {

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

      val actualDf = sourceDf.withColumn(
        "is_even",
        NumberFun.isEvenBrokeUdf(col("number"))
      )

      val expectedSchema = List(
        StructField("number", IntegerType, true),
        StructField("is_even", BooleanType, true)
      )

      val expectedData = List(
        Row(1, false),
        Row(8, true),
        Row(12, true),
        Row(null, null)
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

}

