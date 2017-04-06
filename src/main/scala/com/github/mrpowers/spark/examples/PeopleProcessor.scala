package com.github.mrpowers.spark.examples

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object PeopleProcessor {

  def run(df: DataFrame): DataFrame = {
    df.transform(withAge).transform(withSeniorCitizen)
  }

  def withAge(df: DataFrame): DataFrame = {
    df.withColumn("age", year(current_date()) - col("birth_year"))
  }

  def withSeniorCitizen(df: DataFrame): DataFrame = {
    df.withColumn("senior_citizen", col("age") >= 65)
  }

}
