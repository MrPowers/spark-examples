package com.github.mrpowers.spark.examples

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSpec
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.daria.sql.ColumnExt._
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.types.StringType

object Goo {

def appendABC()(col: Column): Column = {
  concat(col, lit(" ABC"))
}

}

class ColumnChainSpec
    extends FunSpec
    with DataFrameSuiteBase {

  import spark.implicits._

  it("chains column functions") {

    spark.createDF(
      List(
        ("  HI THERE     "),
        (" I lIKe     ")
      ), List(
        ("aaa", StringType, true)
      )
    ).withColumn(
      "aaa",
      col("aaa")
        .chain(lower)
        .chain(trim)
        .chain(Goo.appendABC())
        .chain(regexp_replace(_, "A", "ZZZ"))
    ).show()

  }

  it("chains with rpad") {

    val wordsDf = Seq(
      ("hi  "),
      ("  ok")
    ).toDF("word")

    val actualDf = wordsDf.withColumn(
      "diff_word",
      col("word").chain(trim).chain(rpad(_, 5, "x"))
    )

    actualDf.show()

  }

}
