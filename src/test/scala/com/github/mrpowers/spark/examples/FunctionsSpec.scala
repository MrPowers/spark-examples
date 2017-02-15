package com.github.mrpowers.spark.examples

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.scalatest.FunSpec

class FunctionsSpec extends FunSpec with DataFrameSuiteBase {

  import spark.implicits._

  describe("yeardiff") {

    it("calculates the years between two dates") {

      val testDf = Seq(
        ("2016-09-10", "2001-08-10"),
        ("2016-04-18", "2010-05-18"),
        ("2016-01-10", "2013-08-10")
      )
      .toDF("first_datetime", "second_datetime")
      .withColumn("first_datetime", $"first_datetime".cast("timestamp"))
      .withColumn("second_datetime", $"second_datetime".cast("timestamp"))

      val actualDf = testDf
        .withColumn("num_years", functions.yeardiff(col("first_datetime"), col("second_datetime")))

      val expectedSchema = List(
        StructField("num_years", DoubleType, true)
      )

      val expectedData = Seq(
        Row(15.1),
        Row(5.9),
        Row(2.4)
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameApproximateEquals(actualDf.select("num_years"), expectedDf, 0.1)

    }

  }

}
