package com.github.mrpowers.spark.examples

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSpec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._

class DifferentTypesSpec
    extends FunSpec
    with DataFrameSuiteBase {

  import spark.implicits._

  it("creates an ArrayType column with the split function") {

    val singersDF = Seq(
      ("beatles", "help|hey jude"),
      ("romeo", "eres mia")
    ).toDF("name", "hit_songs")

    val actualDF = singersDF.withColumn(
      "hit_songs",
      split(col("hit_songs"), "\\|")
    )

//    actualDF.show()
//
//    actualDF.printSchema()

  }

  it("directly creates an ArrayType column") {

    val singersDF = spark.createDF(
      List(
        ("bieber", Array("baby", "sorry")),
        ("ozuna", Array("criminal"))
      ), List(
        ("name", StringType, true),
        ("hit_songs", ArrayType(StringType, true), true)
      )
    )

//    singersDF.show()
//
//    singersDF.printSchema()

  }

  it("directly creates a MapType column") {

    val singersDF = spark.createDF(
      List(
        ("sublime", Map(
          "good_song" -> "santeria",
          "bad_song" -> "doesn't exist")
        ),
        ("prince_royce", Map(
          "good_song" -> "darte un beso",
          "bad_song" -> "back it up")
        )
      ), List(
        ("name", StringType, true),
        ("songs", MapType(StringType, StringType, true), true)
      )
    )

//    singersDF.show()
//
//    singersDF.printSchema()
//
//    singersDF
//      .select(
//        col("name"),
//        col("songs")("bad_song").as("bad song!")
//      ).show()

  }

  it("uses both MapType and ArrayType in a single column") {

    val singersDF = spark.createDF(
      List(
        ("miley", Map(
          "good_songs" -> Array("party in the usa", "wrecking ball"),
          "bad_songs" -> Array("younger now"))
          ),
        ("kesha", Map(
          "good_songs" -> Array("tik tok", "timber"),
          "bad_songs" -> Array("rainbow"))
          )
      ), List(
        ("name", StringType, true),
        ("songs", MapType(StringType, ArrayType(StringType, true), true), true)
      )
    )

//    singersDF.show()
//
//    singersDF.printSchema()
//
//    singersDF
//      .select(
//        col("name"),
//        col("songs")("good_songs").as("fun")
//      ).show()

  }

  it("returns true if the array contains an element") {

    val peopleDF = spark.createDF(
      List(
        ("bob", Array("red", "blue")),
        ("maria", Array("green", "red")),
        ("sue", Array("black"))
      ), List(
        ("name", StringType, true),
        ("favorite_colors", ArrayType(StringType, true), true)
      )
    )

    val actualDF = peopleDF.withColumn(
      "likes_red",
      array_contains(col("favorite_colors"), "red")
    )

    actualDF.show()

    peopleDF.select(
      col("name"),
      explode(col("favorite_colors")).as("color")
    ).show()

  }

}
