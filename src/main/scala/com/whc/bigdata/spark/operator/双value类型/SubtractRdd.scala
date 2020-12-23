package com.whc.bigdata.spark.operator.双value类型

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SubtractRdd {
  def main(args: Array[String]): Unit = {
    //创建spark配置对象
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建上下文对象
    val sc: SparkContext = new SparkContext(config)

    val rdd1: RDD[Int] = sc.parallelize(1 to 8)
    val rdd2: RDD[Int] = sc.parallelize(4 to 10)

    //计算2个RDD之间的差集
    val substractrdd: RDD[Int] = rdd1.subtract(rdd2)

    substractrdd.collect().foreach(println)
  }
}
