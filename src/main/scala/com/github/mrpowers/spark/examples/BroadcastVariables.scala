package com.github.mrpowers.spark.examples

import org.apache.spark.sql.functions._

object BroadcastVariables extends Serializable {

  val specialNumbers = Array(8, 13, 17, 19, 23)

  def isSpecial(num: Int): Boolean = {
    specialNumbers.contains(num)
  }

  val is_special_udf = udf[Boolean, Int](isSpecial)

}
