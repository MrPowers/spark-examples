package com.github.mrpowers.spark.examples.chaining

import org.apache.spark.sql._

object MyTransform {

  implicit class CoolStuff(df: DataFrame) {

    def trans(f: (DataFrame) ⇒ DataFrame): DataFrame = {
      f(df)
    }

  }

}
