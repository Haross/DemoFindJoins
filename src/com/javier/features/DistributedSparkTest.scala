package com.javier.features

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DistributedSparkTest {

  def main(args: Array[String]): Unit = {

    val sparkUrl = "192.168.99.119:7077"

    val spark = SparkSession.builder.appName("testDs")
      .master(s"spark://$sparkUrl")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val schemaCVS = StructType(Array(
      StructField("cast", StringType, true),
      StructField("crew", StringType, true),
      StructField("id", IntegerType, true)
    ))


    val path = "/Users/javierflores/filesSpark/"
    val df = spark.read.option("header", "true").schema(schemaCVS).csv(s"${path}credits.csv")

    df.printSchema
    df.filter(col("id").isNull).select(col("id")*2).show(10000)
  }
}
