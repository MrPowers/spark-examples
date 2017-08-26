//package com.github.mrpowers.spark.examples.assumption
//
//import com.holdenkarau.spark.testing.DataFrameSuiteBase
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.functions._
//import org.scalatest._
//import com.github.mrpowers.spark.daria.sql.DataFrameValidator
//import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
//
//class SeniorCitizenSpec
//  extends FunSpec
//  with DataFrameSuiteBase
//  with DataFrameValidator {
//
//  import spark.implicits._
//
//  describe("#example") {
//
//    it("appends a senior citizen column to a DataFrame") {
//      def withIsSeniorCitizen()(df: DataFrame): DataFrame = {
//        df.withColumn("is_senior_citizen", df("age") >= 65)
//      }
//
//      val peopleDF = Seq(
//        ("miguel", 80),
//        ("luisa", 10)
//      ).toDF("name", "age")
//
//      val actualDF = peopleDF.transform(withIsSeniorCitizen())
//
//    }
//
////    it("errors out when the age column isn't present") {
////      def withIsSeniorCitizen()(df: DataFrame): DataFrame = {
////        df.withColumn("is_senior_citizen", df("age") >= 65)
////      }
////
////      val animalDF = Seq(
////        ("cat"),
////        ("dog")
////      ).toDF("age")
////
////      animalDF.show()
////
////      val badDF = animalDF.transform(withIsSeniorCitizen())
////
////      badDF.show()
////    }
//
//  it("errors out when the first_name column isn't present") {
//
//
//    def withFullName()(df: DataFrame): DataFrame = {
//      validatePresenceOfColumns(df, Seq("first_name", "last_name"))
//      df.withColumn(
//        "full_name",
//        concat_ws(" ", col("first_name"), col("last_name"))
//      )
//    }
//
//    val animalDF = Seq(
//      ("cat"),
//      ("dog")
//    ).toDF("age")
//
////    val animalDF = Seq(
////      ("bob", "barker"),
////      ("adam", "sandler")
////    ).toDF("first_name", "last_name")
//
////    val animalDF = Seq(
////      (10, "barker"),
////      (20, "sandler")
////    ).toDF("first_name", "last_name")
//
////    animalDF.show()
//
////    val badDF = animalDF.transform(withFullName())
//
////    badDF.show()
//
//  }
//
//    it("sums columns") {
//
//def withSum()(df: DataFrame): DataFrame = {
//  val requiredSchema = StructType(
//    List(
//      StructField("num1", IntegerType, true),
//      StructField("num2", IntegerType, true)
//    )
//  )
//  validateSchema(df, requiredSchema)
//  df.withColumn(
//    "sum",
//    col("num1") + col("num2")
//  )
//}
//
////val numsDF = Seq(
////  (1, 3),
////  (7, 8)
////).toDF("num1", "num2")
////
////numsDF.transform(withSum()).show()
//
//val wordsDF = Seq(
//  ("one", "three"),
//  ("seven", "eight")
//).toDF("num1", "num2")
//
//wordsDF.transform(withSum()).show()
//
//
//    }
//
//  }
//
//
//}
//
