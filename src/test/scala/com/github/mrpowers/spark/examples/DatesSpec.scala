package com.github.mrpowers.spark.examples

import java.sql.Date
import java.sql.Timestamp

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSpec
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, TimestampType}
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.functions._

class DatesSpec
    extends FunSpec
    with DataFrameSuiteBase {

  describe("DateType") {

    it("creates a DateType column") {

      val sourceDF = spark.createDF(
        List(
          (1, Date.valueOf("2016-09-30")),
          (2, Date.valueOf("2016-12-14"))
        ), List(
          ("person_id", IntegerType, true),
          ("birth_date", DateType, true)
        )
      )

      sourceDF.show()

      sourceDF.printSchema()

    }

  }

  describe("cast") {

    it("converts a StringType column into a DateType column") {

      val sourceDF = spark.createDF(
        List(
          (1, "2013-01-30"),
          (2, "2012-01-01")
        ), List(
          ("person_id", IntegerType, true),
          ("birth_date", StringType, true)
        )
      ).withColumn(
        "birth_date",
        col("birth_date").cast("date")
      )

      sourceDF.show()

      sourceDF.printSchema()

    }

  }

  it("extracts the year, month, and day from a date") {

    val sourceDF = spark.createDF(
      List(
        (1, Date.valueOf("2016-09-30")),
        (2, Date.valueOf("2016-12-14"))
      ), List(
        ("person_id", IntegerType, true),
        ("birth_date", DateType, true)
      )
    )

    sourceDF.withColumn(
      "birth_year",
      year(col("birth_date"))
    ).withColumn(
      "birth_month",
      month(col("birth_date"))
    ).withColumn(
      "birth_day",
      dayofmonth(col("birth_date"))
    ).show()

  }

  it("extracts the minute and second from a timestamp") {

    val sourceDF = spark.createDF(
      List(
        (1, Timestamp.valueOf("2017-12-02 03:04:00")),
        (2, Timestamp.valueOf("1999-01-01 01:45:20"))
      ), List(
        ("person_id", IntegerType, true),
        ("fun_time", TimestampType, true)
      )
    )

    sourceDF.withColumn(
      "fun_minute",
      minute(col("fun_time"))
    ).withColumn(
      "fun_second",
      second(col("fun_time"))
    ).show()

  }

  it("calculates the age in days") {

    val sourceDF = spark.createDF(
      List(
        (1, Date.valueOf("1990-09-30")),
        (2, Date.valueOf("2001-12-14"))
      ), List(
        ("person_id", IntegerType, true),
        ("birth_date", DateType, true)
      )
    )

    sourceDF.withColumn(
      "age_in_days",
      datediff(current_timestamp(), col("birth_date"))
    ).show()

  }

  it("adds days to a date") {

    val sourceDF = spark.createDF(
      List(
        (1, Date.valueOf("1990-09-30")),
        (2, Date.valueOf("2001-12-14"))
      ), List(
        ("person_id", IntegerType, true),
        ("birth_date", DateType, true)
      )
    )

    sourceDF.withColumn(
      "15_days_old",
      date_add(col("birth_date"), 15)
    ).show()

  }

}
