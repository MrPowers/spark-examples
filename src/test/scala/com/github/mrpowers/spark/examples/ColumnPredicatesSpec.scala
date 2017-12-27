package com.github.mrpowers.spark.examples

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSpec
import org.apache.spark.sql.types._
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.github.mrpowers.spark.daria.sql.ColumnExt._
import org.apache.spark.sql.functions._

class ColumnPredicatesSpec
    extends FunSpec
    with DataFrameSuiteBase {

  it("returns true when a column contains a null value") {

    val sourceDF = spark.createDF(
      List(
        (1, "shakira"),
        (2, "sofia"),
        (3, null)
      ), List(
        ("person_id", IntegerType, true),
        ("person_name", StringType, true)
      )
    )

    sourceDF.withColumn(
      "is_person_name_null",
      col("person_name").isNull
    ).show()

  }

  it("returns true when the column does not contain a null value") {

    val sourceDF = spark.createDF(
      List(
        (1, "shakira"),
        (2, "sofia"),
        (3, null)
      ), List(
        ("person_id", IntegerType, true),
        ("person_name", StringType, true)
      )
    )

    sourceDF.withColumn(
      "is_person_name_not_null",
      col("person_name").isNotNull
    ).show()

  }

  it("returns true when the column is in a list of values") {

    val primaryColors = List("red", "yellow", "blue")

    val sourceDF = spark.createDF(
      List(
        ("rihanna", "red"),
        ("solange", "yellow"),
        ("selena", "purple")
      ), List(
        ("celebrity", StringType, true),
        ("color", StringType, true)
      )
    )

    sourceDF.withColumn(
      "is_primary_color",
      col("color").isin(primaryColors: _*)
    ).show()

  }

  it("demonstrates the isTrue and isFalse methods") {

    val sourceDF = spark.createDF(
      List(
        ("Argentina", false),
        ("Japan", true),
        (null, null)
      ), List(
        ("country", StringType, true),
        ("in_asia", BooleanType, true)
      )
    )

    sourceDF.withColumn(
      "in_asia_is_true",
      col("in_asia").isTrue
    ).withColumn(
      "in_asia_is_false",
      col("in_asia").isFalse
    ).show()

  }

  it("demonstrates the isNullOrBlank and isNotNullOrBlank methods") {

    val sourceDF = spark.createDF(
      List(
        ("water"),
        ("  jellyfish"),
        (""),
        ("   "),
        (null)
      ), List(
        ("thing", StringType, true)
      )
    )

    sourceDF.withColumn(
      "is_thing_null_or_blank",
      col("thing").isNullOrBlank
    ).show()

  }

  it("returns true when the column is NOT in a list of values") {

    val primaryColors = List("red", "yellow", "blue")

    val sourceDF = spark.createDF(
      List(
        ("rihanna", "red"),
        ("solange", "yellow"),
        ("selena", "purple")
      ), List(
        ("celebrity", StringType, true),
        ("color", StringType, true)
      )
    )

    sourceDF.withColumn(
      "is_not_primary_color",
      col("color").isNotIn(primaryColors: _*)
    ).show()

  }

  it("returns true if the value is null or false") {

    val sourceDF = spark.createDF(
      List(
        (true),
        (false),
        (null)
      ), List(
        ("likes_cheese", BooleanType, true)
      )
    )

    sourceDF.withColumn(
      "is_likes_cheese_falsy",
      col("likes_cheese").isFalsy
    ).show()

  }

  it("returns true for values other than false and null") {

    val sourceDF = spark.createDF(
      List(
        (1),
        (3),
        (null)
      ), List(
        ("odd_number", IntegerType, true)
      )
    )

    sourceDF.withColumn(
      "is_odd_number_truthy",
      col("odd_number").isTruthy
    ).show()

  }

}
