package com.whc.bigdata.spark.SparkSql.DataFrame

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 与RDD类似，DataFrame也是一个分布式数据容器。然而DataFrame更像传统数据库的二维表格，
 * 除了数据以外，还记录数据的结构信息，即schema。同时，与Hive类似，DataFrame也支持嵌套数据类型（struct、array和map）。
 * 从API易用性的角度上看，DataFrame API提供的是一套高层的关系操作，比函数式的RDD API要更加友好，门槛更低。
 */

object DataFrame {
  def main(args: Array[String]): Unit = {
    //sparkconf
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DataFrame")

    //sparksession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()


    val df: DataFrame = spark.read.json("D:\\ideaproject\\SparkLearning\\src\\main\\scala\\com\\whc\\bigdata\\resources\\user.json")

    //将dataframe转换为一张表
    df.createTempView("user")
    //采用sql的语法访问数据
    val sparkdf: DataFrame = spark.sql("select * from user")

    sparkdf.show()
    spark.stop()


  }
}
