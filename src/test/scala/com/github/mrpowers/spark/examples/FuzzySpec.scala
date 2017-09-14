package com.github.mrpowers.spark.examples

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSpec
import org.apache.spark.sql.functions._

class FuzzySpec
    extends FunSpec
    with DataFrameSuiteBase {

  import spark.implicits._

  it("returns the soundex code") {

    val sourceDF = Seq(
      ("to", "two"),
      ("brake", "break"),
      ("here", "hear"),
      ("tree", "free")
    ).toDF("word1", "word2")

    val actualDF = sourceDF.withColumn(
      "w1_soundex",
      soundex(col("word1"))
    ).withColumn(
      "w2_soundex",
      soundex(col("word2"))
    )

  }

  it("uses soundex to match similar sounding names") {

    val sourceDF = Seq(
      ("dylan", "dillon"),
      ("britney", "britnee"),
      ("crystal", "cristall"),
      ("jim", "bill")
    ).toDF("name1", "name2")

//    sourceDF.show()

    val actualDF = sourceDF.withColumn(
      "name1_name2_soundex_equality",
      soundex(col("name1")) === soundex(col("name2"))
    )

//    actualDF.show()

  }

  it("uses levenshtein to match similar sounding names") {

    val sourceDF = Seq(
      ("blah", "blah"),
      ("cat", "bat"),
      ("phat", "fat"),
      ("kitten", "sitting")
    ).toDF("word1", "word2")

    val actualDF = sourceDF.withColumn(
      "word1_word2_levenshtein",
      levenshtein(col("word1"), col("word2"))
    )

  }

  it("uses the levenshtein when doing DataFrame joins") {

    val largeDF = Seq(
      ("bob", "san diego"),
      ("phil", "seattle"),
      ("juan", "manila"),
      ("joe", "newark")
    ).toDF("firstname", "city")

    largeDF.show()

    val smallDF = Seq(
      ("bobby", "black"),
      ("phillip", "red"),
      ("juanito", "yellow")
    ).toDF("name", "color")

    smallDF.show()

    val joinedDF = largeDF.join(
      smallDF,
      levenshtein(largeDF("firstname"), smallDF("name")) < 4
    )

    joinedDF.show()

  }

}
