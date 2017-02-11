package com.github.mrpowers.spark.examples.basic

import org.apache.spark.sql.functions._

object NumberFun {

  // the with workaround can be used here
  def isEvenSimple(n: Integer): Boolean = {
    n % 2 == 0
  }

  val isEvenSimpleUdf = udf[Boolean, Integer](isEvenSimple)

  // false is incorrectly returned for null values
  def isEvenBad(n: Integer): Boolean = {
    if (n == null) {
      false
    } else {
      n % 2 == 0
    }
  }

  val isEvenBadUdf = udf[Boolean, Integer](isEvenBad)

  // null is correctly returned for null values
  def isEvenBetter(n: Integer): Option[Boolean] = {
    if (n == null) {
      None
    } else {
      Some(n % 2 == 0)
    }
  }

  val isEvenBetterUdf = udf[Option[Boolean], Integer](isEvenBetter)

  // we're following Scala best practices here
  def isEvenOption(n: Integer): Option[Boolean] = {
    val num = Option(n).getOrElse(return None)
    Some(num % 2 == 0)
  }

  val isEvenOptionUdf = udf[Option[Boolean], Integer](isEvenOption)

  // this doesn't work
  def isEvenBroke(n: Option[Integer]): Option[Boolean] = {
    val num = n.getOrElse(return None)
    Some(num % 2 == 0)
  }

  val isEvenBrokeUdf = udf[Option[Boolean], Option[Integer]](isEvenBroke)

}
