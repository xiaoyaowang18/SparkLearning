package com.whc.bigdata.spark.SparkSql.UDF

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object UDFRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UDF")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("D:\\ideaproject\\SparkLearning\\src\\main\\scala\\com\\whc\\bigdata\\resources\\user.json")

    //udf
    spark.udf.register("addName", (x: String) => "Name:" + x)

    df.createOrReplaceTempView("user")

    spark.sql("select addName(name),age from user").show()

    spark.stop()
  }
}
