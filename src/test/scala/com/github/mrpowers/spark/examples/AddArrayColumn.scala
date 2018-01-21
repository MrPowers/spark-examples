package com.github.mrpowers.spark.examples

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSpec
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.daria.sql.functions._

class AddArrayColumn
    extends FunSpec
    with DataFrameSuiteBase {

  import spark.implicits._

  it("parses out the colors from a string fugly") {

    val colors = Array("blue", "red", "pink", "cyan")

    val df = Seq(
      ("i like blue and red"),
      ("you pink and blue")
    ).toDF("word1")

    val actualDF = df.withColumn(
      "colors",
      array(
        when(col("word1").contains("blue"), "blue"),
        when(col("word1").contains("red"), "red"),
        when(col("word1").contains("pink"), "pink"),
        when(col("word1").contains("cyan"), "cyan")
      )
    )

    actualDF.show(truncate=false)

  }

  it("parses out the colors from a string a little better") {

    val df = Seq(
      ("i like blue and red"),
      ("you pink and blue")
    ).toDF("word1")

    val colors = Array("blue", "red", "pink", "cyan")

    val actualDF = df.withColumn(
      "colors",
      array(
        colors.map{ c: String =>
          when(col("word1").contains(c), c)
        }: _*
      )
    )

    actualDF.show(truncate=false)

  }

  it("creates ArrayType columns without nulls") {

    val df = Seq(
      ("i like blue and red"),
      ("you pink and blue")
    ).toDF("word1")

    val actualDF = df.withColumn(
      "colors",
      split(
        concat_ws(
          ",",
          when(col("word1").contains("blue"), "blue"),
          when(col("word1").contains("red"), "red"),
          when(col("word1").contains("pink"), "pink"),
          when(col("word1").contains("cyan"), "cyan")
        ),
        ","
      )
    )

    actualDF.show(truncate=false)

  }

  it("can be cleaned up with arrayExNull") {

    val df = Seq(
      ("i like blue and red"),
      ("you pink and blue")
    ).toDF("word1")

    val colors = Array("blue", "red", "pink", "cyan")

    val actualDF = df.withColumn(
      "colors",
      arrayExNull(
        colors.map{ c: String =>
          when(col("word1").contains(c), c)
        }: _*
      )
    )

    actualDF.show(truncate=false)

  }

}
