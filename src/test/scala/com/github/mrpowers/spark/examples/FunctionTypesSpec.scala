package com.github.mrpowers.spark.examples

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSpec
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.daria.sql.functions._

object AaaBbb {

  def withGreeting()(df: DataFrame): DataFrame = {
    df.withColumn(
      "greeting",
      lit("HEY!")
    )
  }

  def withFavActivity()(df: DataFrame): DataFrame = {
    df.withColumn(
      "fav_activity",
      lit("surfing")
    )
  }

  def toLowerFun(str: String): Option[String] = {
    val s = Option(str).getOrElse(return None)
    Some(s.toLowerCase())
  }

  val toLower = udf[Option[String], String](toLowerFun)

}

class FunctionTypesSpec
    extends FunSpec
    with DataFrameSuiteBase {

  import spark.implicits._

  it("runs order dependent variable assignments") {

    val df = List(
      ("joao"),
      ("gabriel")
    ).toDF("first_name")

    val df2 = df.withColumn(
      "greeting",
      lit("HEY!")
    )

    val df3 = df2.withColumn(
      "fav_activity",
      lit("surfing")
    )

    df3.show()

  }

  it("runs custom transformations") {

    val df = List(
      ("joao"),
      ("gabriel")
    ).toDF("first_name")

    df
      .transform(AaaBbb.withGreeting())
      .transform(AaaBbb.withFavActivity())
      .show()

  }

  it("removes all whitespace") {

    List(
      ("I LIKE food"),
      ("   this    fun")
    ).toDF("words")
      .withColumn(
        "clean_words",
        removeAllWhitespace("words")
      )
      .show()

  }

  it("lowercases strings") {

    val df = List(
      ("HI ThErE"),
      ("ME so HAPPY"),
      (null)
    ).toDF("phrase")

    df
      .withColumn(
        "lower_phrase",
        AaaBbb.toLower(col("phrase"))
      )
      .show()

  }

}
