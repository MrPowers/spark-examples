package com.github.mrpowers.spark.examples.chaining

import org.apache.spark.sql._

object Dance {

  implicit class DataFrameTransforms(df: DataFrame) {

    def salsa(): String = {
      "a fun dance"
    }

  }

}
