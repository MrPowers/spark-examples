package com.github.mrpowers.spark.examples

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

package object functions {

  def yeardiff(end: Column, start: Column): Column = {
    datediff(end, start) / 365
  }

}
