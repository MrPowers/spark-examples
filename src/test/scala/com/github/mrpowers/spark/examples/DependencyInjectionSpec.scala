package com.github.mrpowers.spark.examples

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.FunSpec

class DependencyInjectionSpec
    extends FunSpec
    with DataFrameSuiteBase {

  import spark.implicits._

  it("defines a method that doesn't use any dependency injection") {
    def withStateFullName()(df: DataFrame) = {
      val stateMappingsDF = spark
        .read
        .option("header", true)
        .csv(Config.get("stateMappingsPath"))
      df
        .join(
          broadcast(stateMappingsDF),
          df("state") <=> stateMappingsDF("state_abbreviation"),
          "left_outer"
        )
        .drop("state_abbreviation")
    }

    val df = Seq(
      ("john", 23, "TN"),
      ("sally", 48, "NY")
    ).toDF("first_name", "age", "state")

    df
      .transform(withStateFullName())
      .show()
  }

  it("injects the dependency as a path") {
    def withStateFullNameInjectPath(
      stateMappingsPath: String = Config.get("stateMappingsPath")
    )(df: DataFrame): DataFrame = {
      val stateMappingsDF = spark
        .read
        .option("header", true)
        .csv(stateMappingsPath)
      df
        .join(
          broadcast(stateMappingsDF),
          df("state") <=> stateMappingsDF("state_abbreviation"),
          "left_outer"
        )
        .drop("state_abbreviation")
    }

    val df = Seq(
      ("john", 23, "TN"),
      ("sally", 48, "NY")
    ).toDF("first_name", "age", "state")

    df
      .transform(withStateFullNameInjectPath())
      .show()
  }

  it("defines a method that uses dependency injection") {
    def withStateFullNameInjectDF(
      stateMappingsDF: DataFrame = spark
        .read
        .option("header", true)
        .csv(Config.get("stateMappingsPath"))
    )(df: DataFrame): DataFrame = {
      df
        .join(
          broadcast(stateMappingsDF),
          df("state") <=> stateMappingsDF("state_abbreviation"),
          "left_outer"
        )
        .drop("state_abbreviation")
    }

    val stateMappingsDF = Seq(
      ("Tennessee", "TN"),
      ("New York", "NY")
    ).toDF("state_full_name", "state_abbreviation")

    val df = Seq(
      ("john", 23, "TN"),
      ("sally", 48, "NY")
    ).toDF("first_name", "age", "state")

    df
      .transform(withStateFullNameInjectDF(stateMappingsDF))
      .show()
  }

}
