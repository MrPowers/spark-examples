package com.github.mrpowers.spark.examples

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSpec
import com.github.mrpowers.spark.stringmetric.SimilarityFunctions.jaccard_similarity
import com.github.mrpowers.spark.stringmetric.PhoneticAlgorithms.refined_soundex
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._

class StringmetricsSpec
    extends FunSpec
    with DataFrameSuiteBase {

  describe("string metric algorithm") {

    it("can be used to calculate the jaccard_similarity of two strings") {

      val sourceDF = spark.createDF(
        List(
          ("night", "nacht"),
          ("context", "contact"),
          (null, "nacht"),
          (null, null)
        ), List(
          ("word1", StringType, true),
          ("word2", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "w1_w2_jaccard",
        jaccard_similarity(col("word1"), col("word2"))
      )

      actualDF.show()

    }

  }

  describe("phonetic algorithms") {

    it("can be used to run phonetic algorithms") {

      val sourceDF = spark.createDF(
        List(
          ("night"),
          ("cat"),
          (""),
          (null)
        ), List(
          ("word1", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "word1_refined_soundex",
        refined_soundex(col("word1"))
      )

      actualDF.show()

    }

  }

}
