package com.github.mrpowers.spark.examples.benchmarking

// sample data set downloaded from here: http://seanlahman.com/baseball-archive/statistics/

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object BaseballBatting {

  lazy val spark = SparkSession.builder.getOrCreate()

  val dataPath: String = {
    new java.io.File("./src/main/resources/Batting.csv").getCanonicalPath
  }

  def fluidRun(): Unit = {
    println("***")
    println("Fluid run time")
    time {
      val battingDF = spark.read.option("header", "true").csv(dataPath)
      val sortedDF = battingDF.sort("H")
      val path = new java.io.File("./tmp/sorted_batting").getCanonicalPath
      sortedDF.write.mode(SaveMode.Overwrite).csv(path)
    }
  }

  def run(): Unit = {
    println("***")
    println("STEP 1: Load the DataFrame")
    val battingDF = spark.read.option("header", "true").csv(dataPath)
    time {
      battingDF.cache()
      battingDF.collect().foreach(row => row.mkString)
    }
    println("")
    println("***")
    println("STEP 2: Sort the DataFrame")
    val sortedDF = battingDF.sort("H")
    time {
      sortedDF.cache()
      sortedDF.collect().foreach(row => row.mkString)
    }
    println("")
    println("***")
    println("STEP 3: Write out the DataFrame")
    time {
      val path = new java.io.File("./tmp/sorted_batting").getCanonicalPath
      sortedDF.write.mode(SaveMode.Overwrite).csv(path)
    }
  }

  def time[A](f: => A) = {
    val s = System.nanoTime
    val ret = f
    println("time: "+(System.nanoTime-s)/1e6+"ms")
    ret
  }

}
