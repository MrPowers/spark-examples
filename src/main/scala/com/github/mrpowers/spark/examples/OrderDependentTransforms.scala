package com.github.mrpowers.spark.examples

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.daria.sql.CustomTransform
import com.github.mrpowers.spark.daria.sql.DataFrameExt._

object OrderDependentTransforms {

  def withCountry()(df: DataFrame): DataFrame = {
    df.withColumn(
      "country",
      when(col("city") === "Calgary", "Canada")
        .when(col("city") === "Buenos Aires", "Argentina")
        .when(col("city") === "Cape Town", "South Africa")
    )
  }

  val countryCT = new CustomTransform(
    transform = withCountry(),
    requiredColumns = Seq("city"),
    addedColumns = Seq("country")
  )

  def withHemisphere()(df: DataFrame): DataFrame = {
    df.withColumn(
      "hemisphere",
      when(col("country") === "Canada", "Northern Hemisphere")
        .when(col("country") === "Argentina", "Southern Hemisphere")
        .when(col("country") === "South Africa", "Southern Hemisphere")
    )
  }

  val hemisphereCT = new CustomTransform(
    transform = withHemisphere(),
    requiredColumns = Seq("country"),
    addedColumns = Seq("hemisphere")
  )

  def withHemisphereRefactored()(df: DataFrame): DataFrame = {
    if (df.schema.fieldNames.contains("country")) {
      df.withColumn(
        "hemisphere",
        when(col("country") === "Canada", "Northern Hemisphere")
          .when(col("country") === "Argentina", "Southern Hemisphere")
          .when(col("country") === "South Africa", "Southern Hemisphere")
      )
    } else {
      df
        .transform(withCountry())
        .withColumn(
        "hemisphere",
        when(col("country") === "Canada", "Northern Hemisphere")
          .when(col("country") === "Argentina", "Southern Hemisphere")
          .when(col("country") === "South Africa", "Southern Hemisphere")
      )
    }
  }

  def withHemisphereElegant()(df: DataFrame): DataFrame = {
    def hemTransformation()(df: DataFrame): DataFrame = {
      df.withColumn(
        "hemisphere",
        when(col("country") === "Canada", "Northern Hemisphere")
          .when(col("country") === "Argentina", "Southern Hemisphere")
          .when(col("country") === "South Africa", "Southern Hemisphere")
      )
    }

    if (df.containsColumn("country")) {
      df.transform(hemTransformation())
    } else {
      df
        .transform(withCountry())
        .transform(hemTransformation())
    }
  }

}
