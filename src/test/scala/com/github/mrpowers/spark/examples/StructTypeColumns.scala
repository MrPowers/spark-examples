package com.github.mrpowers.spark.examples

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.FunSpec

object ExampleTransforms {

  def withIsTeenager()(df: DataFrame): DataFrame = {
    df.withColumn("is_teenager", col("age").between(13, 19))
  }

  def withHasPositiveMood()(df: DataFrame): DataFrame = {
    df.withColumn(
      "has_positive_mood",
      col("mood").isin("happy", "glad")
    )
  }

  def withWhatToDo()(df: DataFrame) = {
    df.withColumn(
      "what_to_do",
      when(
        col("is_teenager") && col("has_positive_mood"),
        "have a chat"
      )
    )
  }

}

class StructTypeColumns
    extends FunSpec
    with DataFrameSuiteBase {

  it("creates a DataFrame") {

    val data = Seq(
      Row(1, "a"),
      Row(5, "z")
    )

    val schema = StructType(
      List(
        StructField("num", IntegerType, true),
        StructField("letter", StringType, true)
      )
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    print(df.schema)

  }

  it("allows StructType columns to be appended to DataFrames") {

    val data = Seq(
      Row(20.0, "dog"),
      Row(3.5, "cat"),
      Row(0.000006, "ant")
    )

    val schema = StructType(
      List(
        StructField("weight", DoubleType, true),
        StructField("animal_type", StringType, true)
      )
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    val actualDF = df.withColumn(
      "animal_interpretation",
      struct(
        (col("weight") > 5).as("is_large_animal"),
        col("animal_type").isin("rat", "cat", "dog").as("is_mammal")
      )
    )

    actualDF.show(truncate = false)

    print(actualDF.schema)

    actualDF.printSchema()

    actualDF.select(
      col("animal_type"),
      col("animal_interpretation")("is_large_animal")
        .as("is_large_animal"),
      col("animal_interpretation")("is_mammal")
        .as("is_mammal")
    ).show(truncate = false)

  }

  it("can create order dependent code") {

    val data = Seq(
      Row(30, "happy"),
      Row(13, "sad"),
      Row(18, "glad")
    )

    val schema = StructType(
      List(
        StructField("age", IntegerType, true),
        StructField("mood", StringType, true)
      )
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    df
      .transform(ExampleTransforms.withIsTeenager())
      .transform(ExampleTransforms.withHasPositiveMood())
      .transform(ExampleTransforms.withWhatToDo())
      .show()

    }

  it("can avoid order dependent code with a StructType column") {

    val data = Seq(
      Row(30, "happy"),
      Row(13, "sad"),
      Row(18, "glad")
    )

    val schema = StructType(
      List(
        StructField("age", IntegerType, true),
        StructField("mood", StringType, true)
      )
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    val isTeenager = col("age").between(13, 19)
    val hasPositiveMood = col("mood").isin("happy", "glad")

    df.withColumn(
      "best_action",
      struct(
        isTeenager.as("is_teenager"),
        hasPositiveMood.as("has_positive_mood"),
        when(
          isTeenager && hasPositiveMood,
          "have a chat"
        ).as("what_to_do")
      )
    ).show(truncate = false)


  }

}
