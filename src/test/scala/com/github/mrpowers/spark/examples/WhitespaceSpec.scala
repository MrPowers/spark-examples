package com.github.mrpowers.spark.examples

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSpec
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.daria.sql.functions._

class WhitespaceSpec
    extends FunSpec
    with DataFrameSuiteBase {

  def showQuotes(colName: String): Column = {
    concat(lit("""""""), col(colName), lit("""""""))
  }

  describe("trim") {

    it("removes trailing and leading whitespace") {

      val sourceDF = spark.createDF(
        List(
          ("  a     "),
          ("b     "),
          ("   c"),
          (null)
        ), List(
          ("word", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "trimmed_word",
        trim(col("word"))
      )

      actualDF.withColumn(
        "word",
        showQuotes("word")
      ).withColumn(
        "trimmed_word",
        showQuotes("trimmed_word")
      ).show()

    }

  }

  describe("ltrim") {

    it("removes the leading whitespace") {

      val sourceDF = spark.createDF(
        List(
          ("  a     "),
          ("b     "),
          ("   c"),
          (null)
        ), List(
          ("word", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "ltrimmed_word",
        ltrim(col("word"))
      )

      actualDF.withColumn(
        "word",
        showQuotes("word")
      ).withColumn(
        "ltrimmed_word",
        showQuotes("ltrimmed_word")
      ).show()

    }

  }

  describe("singleSpace") {

    it("replaces all double or triple spaces with a single space") {

      val sourceDF = spark.createDF(
        List(
          ("i  like    cheese"),
          ("  the dog runs   "),
          (null)
        ), List(
          ("words", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "single_spaced",
        singleSpace(col("words"))
      )

      actualDF.withColumn(
        "words",
        showQuotes("words")
      ).withColumn(
        "single_spaced",
        showQuotes("single_spaced")
      ).show()

    }

  }

  describe("removeAllWhitespace") {

    it("removes all whitespace") {

      val sourceDF = spark.createDF(
        List(
          ("i  like    cheese"),
          ("  the dog runs   "),
          (null)
        ), List(
          ("words", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "no_whitespace",
        removeAllWhitespace(col("words"))
      )

      actualDF.withColumn(
        "words",
        showQuotes("words")
      ).withColumn(
        "no_whitespace",
        showQuotes("no_whitespace")
      ).show()

    }

  }

  describe("antiTrim") {

    it("leaves leading and trailing whitespace, but removes inner whitespace") {

      val sourceDF = spark.createDF(
        List(
          ("i  like    cheese"),
          ("  the dog runs   "),
          (null)
        ), List(
          ("words", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "anti_trimmed",
        antiTrim(col("words"))
      )

      actualDF.withColumn(
        "words",
        showQuotes("words")
      ).withColumn(
        "anti_trimmed",
        showQuotes("anti_trimmed")
      ).show()

    }

  }

  describe("rpad") {

    it("adds whitespace to the right of a string") {

      val sourceDF = spark.createDF(
        List(
          ("hi"),
          ("  bye"),
          (null)
        ), List(
          ("word", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "rpadded",
        rpad(col("word"), 10, " ")
      )

      actualDF.withColumn(
        "word",
        showQuotes("word")
      ).withColumn(
        "rpadded",
        showQuotes("rpadded")
      ).show()

    }

  }

  describe("showQuotes") {

    it("adds quotes around strings") {

      val sourceDF = spark.createDF(
        List(
          ("    flying"),
          ("sleeping "),
          (null)
        ), List(
          ("status", StringType, true)
        )
      )

      sourceDF.show()

      sourceDF.withColumn(
        "status",
        showQuotes("status")
      ).show()

    }

  }

}
