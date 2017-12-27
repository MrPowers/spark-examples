package com.github.mrpowers.spark.examples.common_design

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers
import com.github.mrpowers.spark.examples.Config
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.functions._
import org.scalatest.FunSpec

class PatternsSpec
    extends FunSpec
    with DataFrameSuiteBase {

  import spark.implicits._

  it("creates a DataFrame") {

    val sourceDF = Seq(
      ("james", 23),
      ("gretzky", 99)
    ).toDF("last_name", "number")

//    sourceDF.show()
//
//    sourceDF.printSchema()

  }

  it("adds columns to a DataFrame") {

    val sourceDF = Seq(
      ("quibdo"),
      ("manizales")
    ).toDF("city")

    val actualDF = sourceDF.withColumn(
      "country",
      lit("colombia")
    )

    actualDF.show()

  }

  it("adds numbers to a DataFrame column") {

    val sourceDF = Seq(
      (1, 3),
      (3, 3),
      (5, 3)
    ).toDF("num1", "num2")

    val actualDF = sourceDF.withColumn(
      "sum",
      col("num1") + col("num2")
    )

//    actualDF.show()

  }

  it("appends an age_category column for teenagers") {

    val sourceDF = Seq(
      (5),
      (14),
      (19),
      (75)
    ).toDF("age")

    val actualDF = sourceDF.withColumn(
      "age_category",
      when(col("age").between(13, 19), "teenager")
    )

//    actualDF.show()

  }

  it("appends an age_category column for teenagers and children") {

    val sourceDF = Seq(
      (5),
      (14),
      (19),
      (75)
    ).toDF("age")

    val actualDF = sourceDF.withColumn(
      "age_category",
      when(col("age").between(13, 19), "teenager").otherwise(
        when(col("age") <= 7, "young child")
      )
    )

//    actualDF.show()

  }

  it("appends an age_category column for teenagers, children, and elderly folks") {

    val sourceDF = Seq(
      (5),
      (14),
      (19),
      (75)
    ).toDF("age")

    val actualDF = sourceDF.withColumn(
      "age_category",
      when(col("age").between(13, 19), "teenager").otherwise(
        when(col("age") <= 7, "young child").otherwise(
          when(col("age") > 65, "elderly")
        )
      )
    )

//    actualDF.show()

  }

  it("checks if a string isin a list") {

    val primaryColors = List("red", "yellow", "blue")

    val sourceDF = Seq(
      ("rihanna", "red"),
      ("solange", "yellow"),
      ("selena", "purple")
    ).toDF("celebrity", "color")

    val actualDF = sourceDF.withColumn(
      "is_primary_color",
      col("color").isin(primaryColors: _*)
    )

//    actualDF.show()

  }

  it("appends a greeting to a DataFrame") {

    val funDF = Seq(
      ("surfing"),
      ("dancing")
    ).toDF("activity")

    val actualDF = com.github.mrpowers.spark.examples.basic.HelloWorld.withGreeting()(funDF)

//    actualDF.show()

  }

  it("overwrites existing columns in a DataFrame") {

    val sourceDF = Seq(
      ("weird al"),
      ("chris rock")
    ).toDF("person")

    val actualDF = sourceDF.withColumn(
      "person",
      concat(col("person"), lit(" is funny"))
    )

//    actualDF.show()

  }

}
