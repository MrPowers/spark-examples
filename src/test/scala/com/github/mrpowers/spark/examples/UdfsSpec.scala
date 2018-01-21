package com.github.mrpowers.spark.examples

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.StringType
import org.scalatest.FunSpec
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object Whatever {

  def lowerRemoveAllWhitespace(s: String): String = {
    s.toLowerCase().replaceAll("\\s", "")
  }

  val lowerRemoveAllWhitespaceUDF = udf[String, String](lowerRemoveAllWhitespace)

  def betterLowerRemoveAllWhitespace(s: String): Option[String] = {
    val str = Option(s).getOrElse(return None)
    Some(str.toLowerCase().replaceAll("\\s", ""))
  }

  val betterLowerRemoveAllWhitespaceUDF = udf[Option[String], String](betterLowerRemoveAllWhitespace)

  def bestLowerRemoveAllWhitespace()(col: Column): Column = {
    lower(regexp_replace(col, "\\s+", ""))
  }

}

class UdfsSpec extends FunSpec with DataFrameSuiteBase {

  describe("lowercases and removes all whitespace from a string") {

    it("removes trailing and leading whitespace") {

      val sourceDF = spark.createDF(
        List(
          ("  HI THERE     "),
          (" GivE mE PresenTS     ")
        ), List(
          ("aaa", StringType, true)
        )
      )

      sourceDF.select(
        Whatever.lowerRemoveAllWhitespaceUDF(col("aaa")).as("clean_aaa")
      ).show()

      val anotherDF = spark.createDF(
        List(
          ("  BOO     "),
          (" HOO   "),
          (null)
        ), List(
          ("cry", StringType, true)
        )
      )

      // This errors out because of the null value in the cry column
//      anotherDF.select(
//        Whatever.lowerRemoveAllWhitespaceUDF(col("cry")).as("clean_cry")
//      ).show()

    }

    it("doesn't blow up with null values") {

      val anotherDF = spark.createDF(
        List(
          ("  BOO     "),
          (" HOO   "),
          (null)
        ), List(
          ("cry", StringType, true)
        )
      )

      anotherDF.select(
        Whatever.betterLowerRemoveAllWhitespaceUDF(col("cry")).as("clean_cry")
      ).show()

      anotherDF.select(
        Whatever.betterLowerRemoveAllWhitespaceUDF(col("cry")).as("clean_cry")
      ).explain()

    }

  }

  describe("bestLowerRemoveAllWhitespace") {

    it("is the best function for doing the needful") {

      val anotherDF = spark.createDF(
        List(
          ("  BOO     "),
          (" HOO   "),
          (null)
        ), List(
          ("cry", StringType, true)
        )
      )

      anotherDF.select(
        Whatever.bestLowerRemoveAllWhitespace()(col("cry")).as("clean_cry")
      ).show()

      anotherDF.select(
        Whatever.bestLowerRemoveAllWhitespace()(col("cry")).as("clean_cry")
      ).explain()

    }

  }

}
