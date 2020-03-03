package com.javier.features

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object FeaturesA {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      //    .config("spark.driver.bindAddress",21376)
      .getOrCreate()

    import spark.implicits._

    Logger.getLogger("org").setLevel(Level.ERROR)

    val test = spark.read.json("./resources/test.json")
    test.createOrReplaceTempView("test")
    val tabl = spark.sql("SELECT l.numeric1, l.numeric2, l.nominal1, l.nominal2 FROM test LATERAL VIEW explode(array) AS l")
    tabl.show()
    tabl.getMetaFeatures().show()

    spark.stop()
  }

}
