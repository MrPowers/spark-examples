package com.github.mrpowers.spark.examples.basic

import org.apache.spark.sql.functions._

object NumberFun {

  def isEven(n: Integer): Boolean = {
    if (n == null) {
      false
    } else {
      n % 2 == 0
    }
  }

  val isEvenUdf = udf[Boolean, Integer](isEven)

  def isEvenOption(n: Integer): Option[Boolean] = {
    val num = Option(n).getOrElse(return Some(false))
    Some(num % 2 == 0)
  }

  val isEvenOptionUdf = udf[Option[Boolean], Integer](isEvenOption)

}
