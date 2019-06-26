package com.github.mrpowers.spark.examples

import org.apache.spark.sql.types._

import org.scalatest.FunSpec

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.github.mrpowers.spark.daria.sql.DataFrameExt._

import OrderDependentTransforms._

class OrderDependentTransformsSpec extends FunSpec with SparkSessionWrapper {

  it("runs order dependent transformations") {

    val df = spark.createDF(
      List(
        ("Calgary"),
        ("Buenos Aires"),
        ("Cape Town")
      ), List(
        ("city", StringType, true)
      )
    )

    df
      .transform(withCountry())
      .transform(withHemisphere())
      .show()

  }

  describe("withHemisphereRefactored") {

    it("can append the country column intelligently") {

      val df = spark.createDF(
        List(
          ("Calgary"),
          ("Buenos Aires"),
          ("Cape Town")
        ), List(
          ("city", StringType, true)
        )
      )

      df
        .transform(withHemisphereRefactored())
        .show()

    }

    it("doesn't run the withCountry code if it's not needed") {

      val df = spark.createDF(
        List(
          ("Canada"),
          ("Argentina"),
          ("South Africa")
        ), List(
          ("country", StringType, true)
        )
      )

      df
        .transform(withHemisphereRefactored())
        .show()

      df
        .transform(withHemisphereRefactored())
        .explain()

    }

  }

  describe("withHemisphereElegant") {

    it("can append the country column intelligently") {

      val df = spark.createDF(
        List(
          ("Calgary"),
          ("Buenos Aires"),
          ("Cape Town")
        ), List(
          ("city", StringType, true)
        )
      )

      df
        .transform(withHemisphereElegant())
        .show()

      df
        .transform(withHemisphereElegant())
        .explain()

    }

    it("doesn't run the withCountry code if it's not needed") {

      val df = spark.createDF(
        List(
          ("Canada"),
          ("Argentina"),
          ("South Africa")
        ), List(
          ("country", StringType, true)
        )
      )

      df
        .transform(withHemisphereElegant())
        .show()

      df
        .transform(withHemisphereElegant())
        .explain()

    }

  }

  describe("custom transformations") {

    it("runs custom transformations") {

      val df = spark.createDF(
        List(
          ("Calgary"),
          ("Buenos Aires"),
          ("Cape Town")
        ), List(
          ("city", StringType, true)
        )
      )

      df
        .trans(countryCT)
        .trans(hemisphereCT)
        .show()

    }

  }

}
