package com.github.mrpowers.spark.examples.chaining

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object BadImplicit {

  implicit class DataFrameTransforms(df: DataFrame) {

    def withGreeting(): DataFrame = {
      df.withColumn("greeting", lit("hello world"))
    }

    def withFarewell(): DataFrame = {
      df.withColumn("farewell", lit("goodbye"))
    }

    def withHiBye(): DataFrame = {
      df.withGreeting().withFarewell()
    }

  }

}
