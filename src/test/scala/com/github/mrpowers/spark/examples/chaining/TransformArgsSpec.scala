package com.github.mrpowers.spark.examples.chaining

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest._

class TransformArgsSpec extends FunSpec with ShouldMatchers with DataFrameSuiteBase {

  import spark.implicits._

  describe(".withCat") {

    it("adds a cats column to a DataFrame") {

      val sourceDf = Seq(
        "funny",
        "person"
      ).toDF("something")

      val actualDf = TransformArgs.withCat("garfield")(sourceDf)

      val expectedSchema = List(
        StructField("something", StringType, true),
        StructField("cats", StringType, false)
      )

      val expectedData = Seq(
        Row("funny", "garfield meow"),
        Row("person", "garfield meow")
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe(".withGreetingCat") {

    it("adds a cats column to a DataFrame") {

      val sourceDf = Seq(
        "funny",
        "person"
      ).toDF("something")

      val actualDf = TransformArgs.withGreetingCat(sourceDf, "furry")

      val expectedSchema = List(
        StructField("something", StringType, true),
        StructField("greeting", StringType, false),
        StructField("cats", StringType, false)
      )

      val expectedData = Seq(
        Row("funny", "hello world", "furry meow"),
        Row("person", "hello world", "furry meow")
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

}
