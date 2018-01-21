package com.github.mrpowers.spark.examples

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSpec

case class Item(id: String, name: String, companyId: String)

case class Company(companyId: String, name: String, city: String)

class JoinRdds
    extends FunSpec
    with DataFrameSuiteBase {

  it("can join RDDs") {

    val items = spark.sparkContext.parallelize(
      List(
        Item("1", "first", "c1"),
        Item("2", "second", "c1"),
        Item("3", "third", "blah")
      )
    )

    val companies = spark.sparkContext.parallelize(
      List(
        Company("c1", "company-1", "city-1"),
        Company("c2", "company-2", "city-2")
      )
    )

    val joinedRDD = items.keyBy(_.companyId).join(companies.keyBy(_.companyId))

    print("")
    joinedRDD.foreach(println)

  }

}
