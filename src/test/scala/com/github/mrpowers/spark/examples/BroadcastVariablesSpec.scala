package com.github.mrpowers.spark.examples

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSpec
import org.apache.spark.sql.functions._

class BroadcastVariablesSpec
    extends FunSpec
    with DataFrameSuiteBase {

  describe("udfs that rely on variables") {

    it("works on your local machine") {

      val numbersDF = spark.range(30)

      val resultDF = numbersDF.withColumn(
        "is_num_special",
        BroadcastVariables.is_special_udf(col("id"))
      )

      resultDF.show()

    }

    it("also works on a cluster when the variables are broadcasted") {



    }

  }

}
