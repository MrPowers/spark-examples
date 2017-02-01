package com.github.mrpowers.spark.examples.chaining

import org.scalatest._
import com.holdenkarau.spark.testing.DataFrameSuiteBase

import Dance._
//import Sauce._

class ImplicitCollisionSpec extends FunSpec with ShouldMatchers with DataFrameSuiteBase {

  import spark.implicits._

  it("returns a string and doesn't error out") {
    val df = Seq(
      "funny"
    ).toDF("something")

    println("***")
    println(df.salsa())
  }

}
