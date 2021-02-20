package com.whc.bigdata.spark.SparkSql.数据源

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Spark SQL 能够自动推测 JSON数据集的结构，并将它加载为一个Dataset[Row]. 可以通过SparkSession.read.json()去加载一个 一个JSON 文件。
 * 注意：这个JSON文件不是一个传统的JSON文件，每一行都得是一个JSON串。
 */
object json文件 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("readload").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val path = "examples/src/main/resources/people.json"
    val peopleDF = spark.read.json(path)


  }
}
