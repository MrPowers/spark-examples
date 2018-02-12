package com.github.mrpowers.spark.examples

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.functions._
import org.scalatest.FunSpec
import java.util.regex.Pattern

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers


class RlikeSpec
    extends FunSpec
    with DataFrameSuiteBase {

  import spark.implicits._

  it("finds when a string contains a substring") {

    val keywords = Array(
      "was  found",
      "WAS detected"
    )

    val cleanKeywords = keywords.map { (s: String) =>
      s.toLowerCase().replaceAll("\\s", "")
    }

    val df = List(
      ("A THING was FOUND"),
      ("It was detected"),
      ("I like movies")
    ).toDF("comments")
      .withColumn(
        "clean_comments",
        lower(regexp_replace(col("comments"), "\\s+", ""))
      )
      .withColumn(
        "contains_keyword",
        col("clean_comments").rlike(cleanKeywords.mkString("|"))
      )

    df.show(truncate = false)

  }

  it("detects when a strings contains a word") {

    val df = List(
      ("the cat and the hat"),
      ("i love your cat"),
      ("dogs are cute"),
      ("pizza please")
    ).toDF("phrase")

    df
      .withColumn(
        "contains_cat",
        col("phrase").rlike("cat")
      )
      .show(truncate = false)

  }

  it("detects when the string contains multiple words") {

    val df = List(
      ("the cat and the hat"),
      ("i love your cat"),
      ("dogs are cute"),
      ("pizza please")
    ).toDF("phrase")

    df
      .withColumn(
        "contains_cat_or_dog",
        col("phrase").rlike("cat|dog")
      )
      .show(truncate = false)

  }

  it("works when the animals are abstracted to a list") {

    val df = List(
      ("the cat and the hat"),
      ("i love your cat"),
      ("dogs are cute"),
      ("pizza please")
    ).toDF("phrase")

    val animals = List("cat", "dog")

    df
      .withColumn(
        "contains_cat_or_dog",
        col("phrase").rlike(animals.mkString("|"))
      )
      .show(truncate = false)

  }

  it("matches strings that start with a certain criteria") {

    val df = List(
      ("i like tacos"),
      ("i want love"),
      ("pie is what i like"),
      ("pizza pizza"),
      ("you like pie")
    ).toDF("phrase")

    df
      .withColumn(
        "starts_with_desire",
        col("phrase").rlike("^i like|^i want")
      )
      .show(truncate = false)

  }

  it("matches strings that end with a certain criteria") {

    val df = List(
      ("i like tacos"),
      ("i want love"),
      ("pie is what i like"),
      ("pizza pizza"),
      ("you like pie")
    ).toDF("phrase")

    val foods = List("tacos", "pizza", "pie")

    df
      .withColumn(
        "ends_with_food",
        col("phrase").rlike(foods.map(_ + "$").mkString("|"))
      )
      .show(truncate = false)

  }

  it("gets messed up with special regexp characters") {

    val df = List(
      ("fun|stuff"),
      ("dancing is fun"),
      ("you have stuff"),
      ("where is fun|stuff"),
      ("something else")
    ).toDF("phrase")

    df
      .withColumn(
        "contains_fun_pipe_stuff",
        col("phrase").rlike("fun|stuff")
      )
      .show(truncate = false)

  }

  it("works when the regexp character is escaped") {

    val df = List(
      ("fun|stuff"),
      ("dancing is fun"),
      ("you have stuff"),
      ("where is fun|stuff"),
      ("something else")
    ).toDF("phrase")

    println("THIS ONE")

    df
      .withColumn(
        "contains_fun_pipe_stuff",
        col("phrase").rlike("fun\\|stuff")
      )
      .show(truncate = false)


  }

  it("works properly when pattern quoting is used") {

    val df = List(
      ("fun|stuff"),
      ("dancing is fun"),
      ("you have stuff"),
      ("where is fun|stuff"),
      ("something else")
    ).toDF("phrase")

    df
      .withColumn(
        "contains_fun_pipe_stuff",
        col("phrase").rlike(Pattern.quote("fun|stuff"))
      )
      .show(truncate = false)

  }


  it("omg") {

    val df = List(
      ("coffee is good"),
      ("i need coffee"),
      ("bread is good"),
      ("i need bread"),
      ("you're a nice|person"),
      ("that is nice")
    ).toDF("phrase")

    val weirdMatchesPath = new java.io.File(s"./src/test/resources/random_matches.csv").getCanonicalPath

    val weirdMatchesDF = spark
      .read
      .option("header", "true")
      .option("charset", "UTF8")
      .csv(weirdMatchesPath)

    val matchString = DataFrameHelpers.columnToArray[String](
      weirdMatchesDF,
      "match_criteria"
    ).mkString("|")

    df
      .withColumn(
        "weird_matches",
        col("phrase").rlike(matchString)
      )
      .show(truncate = false)

  }

}






