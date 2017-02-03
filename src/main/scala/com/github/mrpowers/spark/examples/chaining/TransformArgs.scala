package com.github.mrpowers.spark.examples.chaining

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object TransformArgs {

  def withGreeting(df: DataFrame): DataFrame = {
    df.withColumn("greeting", lit("hello world"))
  }

  def withCat(name: String)(df: DataFrame): DataFrame = {
    df.withColumn("cats", lit(s"$name meow"))
  }

  def withGreetingCat(df: DataFrame, name: String): DataFrame = {
    df
      .transform(withGreeting)
      .transform(withCat(name))
  }

}
