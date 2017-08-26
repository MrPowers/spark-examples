package com.github.mrpowers.spark.examples.testable

import com.github.mrpowers.spark.daria.sql.DataFrameValidator
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSpec
import org.apache.spark.sql.functions._

class Surfing
  extends FunSpec
    with DataFrameSuiteBase
    with DataFrameValidator {

  import spark.implicits._

//  it("shows what we don't want") {
//
//    val sourceDF = Seq(
//      (2),
//      (10)
//    ).toDF("wave_height")
//
//    val funDF = sourceDF.withColumn(
//      "stoke_level",
//      when($"wave_height" > 6, "radical").otherwise("bummer")
//    )
//
//    funDF.show()
//
//  }

//  it("can be done with a DataFrame transformation") {
//
//    val sourceDF = Seq(
//      (2),
//      (10)
//    ).toDF("wave_height")
//
//    def withStokeLevel()(df: DataFrame): DataFrame = {
//      df.withColumn(
//        "stoke_level",
//        when($"wave_height" > 6, "radical").otherwise("bummer")
//      )
//    }
//
//    val funDF = sourceDF.transform(withStokeLevel())
//
//    funDF.show()
//
//  }

//  it("shows ugly code without a UDF") {
//
//    val sourceDF = Seq(
//      ("tulips are pretty"),
//      ("apples are yummy")
//    ).toDF("joys")
//
//    val happyDF = sourceDF.withColumn(
//      "joys_contains_tulips",
//      $"joys".contains("tulips")
//    )
//
//    happyDF.show()
//
//  }

//  it("shows better code with a UDF") {
//
//    val sourceDF = Seq(
//      ("tulips are pretty"),
//      ("apples are yummy")
//    ).toDF("joys")
//
//    def containsTulips(str: String): Boolean = {
//      str.contains("tulips")
//    }
//
//    val containsTulipsUDF = udf[Boolean, String](containsTulips)
//
//    val happyDF = sourceDF.withColumn(
//      "joy_contains_tulips",
//      containsTulipsUDF($"joys")
//    )
//
//    happyDF.show()
//
//  }

//  it("shouldn't make DataFrame transformations that do two things") {
//
//    def doesStuff()(df: DataFrame): DataFrame = {
//      df
//        .filter($"num" % 2 === 0)
//        .withColumn("food", lit("rum ham"))
//    }
//
//    val sourceDF = Seq(
//      (2),
//      (5),
//      (10)
//    ).toDF("num")
//
//    val sunnyDF = sourceDF.transform(doesStuff())
//
//    sunnyDF.show()
//
//  }

//  it("uses single purpose DataFrame transformations") {
//
//    def filterOddNumbers()(df: DataFrame): DataFrame = {
//      df.filter($"num" % 2 === 0)
//    }
//
//    def withRumHam()(df: DataFrame): DataFrame = {
//      df.withColumn("food", lit("rum ham"))
//    }
//
//    val sourceDF = Seq(
//      (2),
//      (5),
//      (10)
//    ).toDF("num")
//
//    val sunnyDF = sourceDF
//      .transform(filterOddNumbers())
//      .transform(withRumHam())
//
//    sunnyDF.show()
//
//  }

}
