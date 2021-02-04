package com.whc.bigdata.spark.SparkCore.operator.双value类型

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object IntersectionRdd {
  def main(args: Array[String]): Unit = {
    //创建spark配置对象呀
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建上下文对象
    val sc: SparkContext = new SparkContext(config)

    val rdd1: RDD[Int] = sc.parallelize(1 to 8)
    val rdd2: RDD[Int] = sc.parallelize(4 to 10)

    //求两个RDD之间的交集
    val intersectionrdd: RDD[Int] = rdd1.intersection(rdd2)

    intersectionrdd.collect().foreach(println)

  }
}
