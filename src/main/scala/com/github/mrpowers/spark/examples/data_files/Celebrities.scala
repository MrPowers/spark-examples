package com.github.mrpowers.spark.examples.data_files

import com.github.mrpowers.spark.examples.{Config, SparkSessionWrapper}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Celebrities extends SparkSessionWrapper {

  def withStateAbbreviation()(df: DataFrame): DataFrame = {

    val stateMappingsDF = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("charset", "UTF8")
      .load(Config.get("stateMappingsPath"))

    df.join(
      broadcast(stateMappingsDF),
      Seq("medivo_test_result_type"),
      "left_outer"
    )

  }

}
