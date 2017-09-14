package com.github.mrpowers.spark.examples

import com.github.mrpowers.spark.daria.sql.DataFrameValidator
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.FunSpec

class SchemaIndependentTransformationsSpec
    extends FunSpec
    with DataFrameSuiteBase
    with DataFrameValidator {

  import spark.implicits._

  it("demonstrates a schema dependent transformation") {

    val kittensDF = List(
      ("tiger", 1),
      ("lily", 2),
      ("jack", 4)
    ).toDF("name", "age")

    kittensDF.show()

    def withAgePlusOne()(df: DataFrame): DataFrame = {
      df.withColumn("age_plus_one", $"age" + 1)
    }

    kittensDF.transform(withAgePlusOne()).show()


  }

  it("demonstrates a schema independent transformation") {

    val puppyDF = List(
      ("max", 5),
      ("charlie", 6),
      ("daisy", 7)
    ).toDF("pup_name", "pup_age")

    puppyDF.show()

    def withAgePlusOne(
      ageColName: String,
      resultColName: String
    )(df: DataFrame): DataFrame = {
      validatePresenceOfColumns(df, Seq(ageColName))
      df.withColumn(resultColName, col(ageColName) + 1)
    }

    puppyDF.transform(
      withAgePlusOne("pup_age", "pup_age_plus_one")
    ).show()

  }

  it("demonstrates when schema dependent transformations are useful") {

    val sourceDF = List(
      ("bob", "barker", "1234", "11111"),
      ("rich", "piano", "4444", "22222")
    ).toDF("fname", "lname", "ssn", "zip")

    sourceDF.show()

    def withPersonId()(df: DataFrame): DataFrame = {
      sourceDF.withColumn(
        "person_id",
        md5(concat_ws(",", $"fname", $"lname", $"ssn", $"zip"))
      )
    }

    sourceDF.transform(withPersonId()).show()

  }

  it("demonstrates that schema independent transformations can be bulky") {

    val sourceDF = List(
      ("bob", "barker", "1234", "11111"),
      ("rich", "piano", "4444", "22222")
    ).toDF("fname", "lname", "ssn", "zip")

    sourceDF.show()

    def withPersonId(
      fnameColName: String = "fname",
      lnameColName: String = "lname",
      ssnColName: String = "ssn",
      zipColName: String = "zip"
    )(df: DataFrame): DataFrame = {
      sourceDF.withColumn(
        "person_id",
        md5(
          concat_ws(
            ",",
            col(fnameColName),
            col(lnameColName),
            col(ssnColName),
            col(zipColName)
          )
        )
      )
    }

    sourceDF.transform(
      withPersonId()
    ).show()

  }

}
