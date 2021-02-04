package com.whc.bigdata.spark.SparkCore.operator.双value类型

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CartesianRdd {
  def main(args: Array[String]): Unit = {
    //创建spark配置对象
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建上下文对象
    val sc: SparkContext = new SparkContext(config)

    val rdd1: RDD[Int] = sc.parallelize(1 to 3)
    val rdd2: RDD[Int] = sc.parallelize(2 to 4)

    //计算两个RDD之间的笛卡尔积
    val cartesianrdd: RDD[(Int, Int)] = rdd1.cartesian(rdd2)

    cartesianrdd.collect().foreach(println)
  }
}
