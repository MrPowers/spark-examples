package com.github.mrpowers.spark.examples.benchmarking

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest._

class BaseballBattingSpec extends FunSpec with DataFrameSuiteBase {

  describe(".fluidRun") {

    it("does a fluid run without breaks") {
      BaseballBatting.fluidRun()
    }

  }

  describe(".run") {

    it("reads the CSV file and outputs sorted files") {
      BaseballBatting.run()
    }

  }

}
