package com.github.mrpowers.spark.examples.fold

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.FunSpec

import com.github.mrpowers.spark.daria.sql.functions._

class CoolioSpec
    extends FunSpec
    with DataFrameSuiteBase {

  import spark.implicits._

  it("removes whitespace from multiple columns") {

    val sourceDF = Seq(
      ("  p a   b l o", "Paraguay"),
      ("Neymar", "B r    asil")
    ).toDF("name", "country")

    val actualDF = Seq(
      "name",
      "country"
    ).foldLeft(sourceDF) { (memoDF, colName) =>
      memoDF.withColumn(
        colName,
        regexp_replace(col(colName), "\\s+", "")
      )
    }

    actualDF.show()

  }

  it("does a better job removing whitespace from multiple columns") {

    val sourceDF = Seq(
      ("  p a   b l o", "Paraguay"),
      ("Neymar", "B r    asil")
    ).toDF("name", "country")

    val actualDF = sourceDF
      .columns
      .foldLeft(sourceDF) { (memoDF, colName) =>
        memoDF.withColumn(
          colName,
          removeAllWhitespace(col(colName))
        )
      }

    actualDF.show()

  }

  it("snake_cases the column names") {

    val sourceDF = Seq(
      ("funny", "joke")
    ).toDF("A b C", "de F")

    sourceDF.show()

    val actualDF = sourceDF
      .columns
      .foldLeft(sourceDF) { (memoDF, colName) =>
        memoDF
          .withColumnRenamed(
            colName,
            colName.toLowerCase().replace(" ", "_")
          )
      }

    actualDF.show()

  }

}
