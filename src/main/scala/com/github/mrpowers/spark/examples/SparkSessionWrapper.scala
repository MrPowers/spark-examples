package com.github.mrpowers.spark.examples

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark examples")
      .getOrCreate()
  }

}


