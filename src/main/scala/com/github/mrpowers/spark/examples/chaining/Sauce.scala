package com.github.mrpowers.spark.examples.chaining

import org.apache.spark.sql._

object Sauce {

  implicit class DataFrameTransforms(df: DataFrame) {

    def salsa(): String = {
      "a delicious sauce"
    }

  }


}