package com.javier.features

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object FeaturesA {

  lazy val schema1 = StructType(Array(
    StructField("id", IntegerType, true),
    StructField("edad", IntegerType, true),
    StructField("sexo", StringType, true)
  ))

  lazy val schema2 = StructType(Array(
    StructField("id", IntegerType, true),
    StructField("nombre", StringType, true),
    StructField("ocupacion", StringType, true)
  ))

  lazy val schema3 = StructType(Array(
    StructField("identifier", IntegerType, true),
    StructField("name", StringType, true),
    StructField("age", IntegerType, true),
    StructField("color", StringType, true)
  ))


  def main(args: Array[String]) {

    val opt = Seq("findJoins", "metaFeatures")

    if (args.length == 0) {
      println("\n  I need one parameter within the following options: "+opt)
      System.exit(0)
    }

    val spark = SparkSession.builder.appName("SparkSQL")
      .master("local[*]")
      .config("spark.driver.bindAddress","127.0.0.1")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
//    import spark.implicits._

    "findJoins" match{

      case "findJoins" =>
        val df1 = spark.read.option("header", "true").schema(schema1).csv("./resources/testJoin1.csv")
        df1.show
        val df2 = spark.read.option("header", "true").schema(schema2).csv("./resources/testJoin2.csv")
        df2.show
        val df3 = spark.read.option("header", "true").schema(schema3).csv("./resources/testJoin3.csv")
        df3.show

        val lista = Seq(df2, df3)
        df1.findJoins(df2,df3)
//        df1.findJoins(lista)
//      OUTPUT:
//        computing metadata for main dataset
//        computing metadata for dataset 0 in the sequence
//        computing metadata for dataset 1 in the sequence
//
//             DATAFRAMES 				 CANDIDATES ATTRIBUTES 			 JOIN-SCORE
//        Main DF and DataFrame[0] 	 main.id and DataFrame[0].id 		 0.0
//        Main DF and DataFrame[0] 	 main.edad and DataFrame[0].nombre 		 0.0
//        Main DF and DataFrame[0] 	 main.sexo and DataFrame[0].ocupacion 		 0.0
//        Main DF and DataFrame[1] 	 main.id and DataFrame[1].id 		 0.0
//        Main DF and DataFrame[1] 	 main.edad and DataFrame[1].nombre 		 0.0
//        Main DF and DataFrame[1] 	 main.sexo and DataFrame[1].edad 		 0.0

        df1.computeMetaFeatures

      case "metaFeatures"  =>
        val test = spark.read.json("./resources/test.json")
        test.createOrReplaceTempView("test")
        val tabl = spark.sql("SELECT l.numeric1, l.numeric2, l.nominal1, l.nominal2 FROM test LATERAL VIEW explode(array) AS l")
        tabl.show()
        tabl.computeMetaFeatures.show()
//        tabl.showJoinPredicates()

//        Result:
//
//    Dimensionality: 1
//    Number of nominal attributes: 2
//    Number of numeric attributes: 2
//    Percentage of nominal attributes: 50
//    Percentage of numeric attributes: 50
//    Average of nominal values: 1.0
//    Std of nominal values: 0.0
//    Min number of nominal values: 1.0
//    Max number of nominal values: 1.0
//    Average of numeric values: 4.125
//    Std of numeric values: 2.6516504294495533
//    Min of numeric values: 2.25
//    Max of numeric values: 6.0
//    Missing attribute count: 2.0
//    Missing attribute percentage: 50.0
//    Min number of missing values: 0.0
//    Max number of missing values: 2.0
//    Min percentage of missing values: 0.0
//    Max percentage of missing values: 50.0
//    Mean number of missing values: 0.75
//    Mean percentage of missing values: 0.75
//
//    +-------------------+------------------+------------------+-------------------+-------------------+
//    |        metaFeature|          nominal1|          nominal2|           numeric1|           numeric2|
//    +-------------------+------------------+------------------+-------------------+-------------------+
//    |          range_val|              null|              null|               4, 8|               1, 3|
//    | missing_values_pct|               0.0|              25.0|               50.0|                0.0|
//    |        val_pct_min|              50.0|              50.0|               null|               null|
//    |               mean|              null|              null|                6.0|               2.25|
//    |     val_pct_median|                 1|                 1|               null|               null|
//    |                std|              null|              null| 2.8284271247461903| 0.9574271077563381|
//    |       val_size_min|                 1|                 1|               null|               null|
//    |       val_size_avg|1.3333333333333333|1.3333333333333333|               null|               null|
//    |            min_val|              null|              null|                  4|                  1|
//    |        val_pct_max|              50.0|              50.0|               null|               null|
//    |distinct_values_cnt|                 1|                 1|                  3|                  3|
//    |        val_pct_std|              50.0|              50.0|               null|               null|
//    |          co_of_var|              null|              null|0.47140452079103173|0.42552315900281695|
//    |distinct_values_pct|              25.0|              25.0|               75.0|               75.0|
//    |       val_size_std|0.5773502691896257|0.5773502691896257|               null|               null|
//    |       val_size_max|                 2|                 2|               null|               null|
//    +-------------------+------------------+------------------+-------------------+-------------------+

      case _ =>
        println("\n No valid option, try one of the followings: "+opt)

    }


    spark.stop()
  }

}
