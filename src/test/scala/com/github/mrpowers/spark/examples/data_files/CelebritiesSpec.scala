package com.github.mrpowers.spark.examples.data_files

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers
import com.github.mrpowers.spark.examples.Config
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.functions._
import org.scalatest.FunSpec

class CelebritiesSpec
    extends FunSpec
    with DataFrameSuiteBase {

  import spark.implicits._

  it("joins with a data file") {

    val sourceDF = Seq(
      ("britney spears", "Mississippi"),
      ("romeo santos", "New York"),
      ("miley cyrus", "Tennessee"),
      ("random dude", null),
      (null, "Dubai")
    ).toDF("name", "birth_state")

    val stateMappingsDF = spark
      .read
      .option("header", "true")
      .option("charset", "UTF8")
      .csv(Config.get("stateMappingsPath"))

    val resultDF = sourceDF.join(
      broadcast(stateMappingsDF),
      sourceDF("birth_state") <=> stateMappingsDF("state_name"),
      "left_outer"
    ).drop(stateMappingsDF("state_name"))

    resultDF.show()

  }

  it("allows data file columns to be accessed as arrays") {

    val stateMappingsDF = spark
      .read
      .option("header", "true")
      .option("charset", "UTF8")
      .csv(Config.get("stateMappingsPath"))

    val abbreviations = DataFrameHelpers.columnToArray[String](
      stateMappingsDF,
      "state_abbreviation"
    )

    println(abbreviations.mkString(" "))

    val sourceDF = Seq(
      ("NY"),
      ("nowhere"),
      (null)
    ).toDF("state_abbr")

    val resultDF = sourceDF.withColumn(
      "is_valid_state_abbr",
      col("state_abbr").isin(abbreviations: _*)
    )

    resultDF.show()

  }

}
