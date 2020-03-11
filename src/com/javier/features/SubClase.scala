package com.javier.features

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
    StructField("id", IntegerType, true),
    StructField("nombre", StringType, true),
    StructField("edad", IntegerType, true),
    StructField("color", StringType, true)
  ))



  def main(args: Array[String]) {

    val opt = Seq("oneJoin","multipleJoins", "metaFeatures")
    if (args.length == 0) {
      println("\n dude, i need one parameter within the following options: "+opt)
      System.exit(0)
    }

    val spark = SparkSession.builder.appName("SparkSQL")
      .master("local[*]")
      //    .config("spark.driver.bindAddress",21376)
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._

    args(0) match{
      case "oneJoin" =>
        val df1 = spark.read.option("header", "true").schema(schema1).csv("./resources/testJoin1.csv")
        df1.show
        val df2 = spark.read.option("header", "true").schema(schema2).csv("./resources/testJoin2.csv")
        df2.show

        df1.findJoins(df2).show()

//        result:
//        +---+----+----+---+--------+-----------+
//        | id|edad|sexo| id|  nombre|  ocupacion|
//        +---+----+----+---+--------+-----------+
//        |  1|  18|   M|  1|  Javier| estudiante|
//        |  2|  20|   F|  2|Angelica|programador|
//        |  3|  18|   M|  3| Edgardo| estudiante|
//        |  4|  22|   F|  4| Krystel|programador|
//        |  5|  23|   M|  5| Ernesto| estudiante|
//        +---+----+----+---+--------+-----------+



      case "multipleJoins" =>
        val df1 = spark.read.option("header", "true").schema(schema1).csv("./resources/testJoin1.csv")
        df1.show
        val df2 = spark.read.option("header", "true").schema(schema2).csv("./resources/testJoin2.csv")
        df2.show
        val df3 = spark.read.option("header", "true").schema(schema3).csv("./resources/testJoin3.csv")
        df3.show

        val lista = df1.findJoinsFromMultipleDF(df2,df3)
        lista.map(_.show)

//        result:
//        +---+----+----+---+--------+-----------+
//        | id|edad|sexo| id|  nombre|  ocupacion|
//        +---+----+----+---+--------+-----------+
//        |  1|  18|   M|  1|  Javier| estudiante|
//        |  2|  20|   F|  2|Angelica|programador|
//        |  3|  18|   M|  3| Edgardo| estudiante|
//        |  4|  22|   F|  4| Krystel|programador|
//        |  5|  23|   M|  5| Ernesto| estudiante|
//        +---+----+----+---+--------+-----------+
//
//        +---+----+----+---+--------+----+------+
//        | id|edad|sexo| id|  nombre|edad| color|
//        +---+----+----+---+--------+----+------+
//        |  3|  18|   M|  1|  Javier|  18|  rojo|
//        |  1|  18|   M|  1|  Javier|  18|  rojo|
//        |  2|  20|   F|  2|Angelica|  20|  azul|
//        |  3|  18|   M|  3| Edgardo|  18|blanco|
//        |  1|  18|   M|  3| Edgardo|  18|blanco|
//        |  4|  22|   F|  4| Krystel|  22|  rojo|
//        |  5|  23|   M|  5| Ernesto|  23| verde|
//        +---+----+----+---+--------+----+------+
//
//        +---+--------+-----------+---+--------+----+------+
//        | id|  nombre|  ocupacion| id|  nombre|edad| color|
//        +---+--------+-----------+---+--------+----+------+
//        |  1|  Javier| estudiante|  1|  Javier|  18|  rojo|
//        |  2|Angelica|programador|  2|Angelica|  20|  azul|
//        |  3| Edgardo| estudiante|  3| Edgardo|  18|blanco|
//        |  4| Krystel|programador|  4| Krystel|  22|  rojo|
//        |  5| Ernesto| estudiante|  5| Ernesto|  23| verde|
//        +---+--------+-----------+---+--------+----+------+


      case "metaFeatures"  =>
        val test = spark.read.json("./resources/test.json")
        test.createOrReplaceTempView("test")
        val tabl = spark.sql("SELECT l.numeric1, l.numeric2, l.nominal1, l.nominal2 FROM test LATERAL VIEW explode(array) AS l")
        tabl.show()
        tabl.metaFeatures.show()

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
