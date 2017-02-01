package com.github.mrpowers.spark.examples.chaining

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest._

class TransformMethodSpec extends FunSpec with ShouldMatchers with DataFrameSuiteBase {

  import spark.implicits._

  describe(".withHiBye") {

    it("adds greeting and farewell columns to a DataFrame") {

      val sourceDf = Seq(
        "funny",
        "person"
      ).toDF("something")

      val actualDf = TransformMethod.withHiBye(sourceDf)

      val expectedSchema = List(
        StructField("something", StringType, true),
        StructField("greeting", StringType, false),
        StructField("farewell", StringType, false)
      )

      val expectedData = Seq(
        Row("funny", "hello world", "goodbye"),
        Row("person", "hello world", "goodbye")
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

}
