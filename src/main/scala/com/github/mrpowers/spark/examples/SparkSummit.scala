package com.github.mrpowers.spark.examples

import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame

object SparkSummit extends SparkSessionWrapper {

  import spark.implicits._

def createParquetLake() = {

val schema = StructType(
  Seq(
    StructField("name", StringType, true)
  )
)

val sDF = spark.readStream
  .schema(schema)
  .csv("/my-cool-bucket/csv-lake/data")

sDF
  .writeStream
  .trigger(Trigger.Once)
  .format("parquet")
  .option("checkpointLocation", "/my-cool-bucket/parquet-lake/checkpoint")
  .start("/my-cool-bucket/parquet-lake/data/incremental")

}

def compactParquetLake() = {

val df = spark
  .read
  .parquet("/my-cool-bucket/parquet-lake/data/incremental")

df
  .coalesce(166)
  .write
  .mode(SaveMode.Append)
  .parquet("/my-cool-bucket/parquet-lake/data/base")

}

def accessDataLake() = {

spark
  .read
  .parquet("/my-cool-bucket/parquet-lake/data/{incremental,base}")

}

def filteringUnpartitionedLake() = {

val df = spark
  .read
  .option("header", "true")
  .csv("/Users/powers/Documents/tmp/blog_data/people.csv")

df
  .where($"country" === "Russia" && $"first_name".startsWith("M"))
  .explain()

}

def createPartitionedDataLake() = {

val df = spark
  .read
  .option("header", "true")
  .csv("/Users/powers/Documents/tmp/blog_data/people.csv")

df
  .repartition($"country")
  .write
  .option("header", "true")
  .partitionBy("country")
  .csv("/Users/powers/Documents/tmp/blog_data/partitioned_lake")

}

def directlyGrabbingPartitions() = {

val russiansDF = spark
  .read
  .csv("/Users/powers/Documents/tmp/blog_data/partitioned_lake/country=Russia")

russiansDF.where($"first_name".startsWith("M"))

}

def writeEachPartitionAsSingleFile(df: DataFrame) = {

// each partition is single file
df
  .repartition($"country")
  .write
  .option("header", "true")
  .partitionBy("country")
  .csv("/Users/powers/Documents/tmp/blog_data/partitioned_lake")

}

def writeEachPartitionsAsTonsOfFiles(df: DataFrame) = {

// tons of files get written out
df
  .write
  .option("header", "true")
  .partitionBy("country")
  .csv("/Users/powers/Documents/tmp/blog_data/partitioned_lake")

}

def maxHundredFilesPerPartition(df: DataFrame) = {

import org.apache.spark.sql.functions.rand

// max 100 files per partition
df
  .repartition(100, $"country", rand)
  .write
  .option("header", "true")
  .partitionBy("country")
  .csv("/Users/powers/Documents/tmp/blog_data/partitioned_lake")

}

// create table
spark.sql("CREATE TABLE delta_pond_for_spike USING DELTA LOCATION '/mnt/some-bucket/delta/pond'")

// compaction
spark.sql("OPTIMIZE delta_pond_for_spike")

// clean up files associated with table
spark.sql("VACUUM delta_pond_for_spike")

}
