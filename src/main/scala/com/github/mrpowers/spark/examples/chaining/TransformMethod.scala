package com.github.mrpowers.spark.examples.chaining

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object TransformMethod {

  def withGreeting(df: DataFrame): DataFrame = {
    df.withColumn("greeting", lit("hello world"))
  }

  def withFarewell(df: DataFrame): DataFrame = {
    df.withColumn("farewell", lit("goodbye"))
  }

  def withHiBye(df: DataFrame): DataFrame = {
    df
      .transform(withGreeting)
      .transform(withFarewell)
  }

}
