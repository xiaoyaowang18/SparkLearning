package com.whc.bigdata.spark.SparkCore.operator.双value类型

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object UnionRdd {
  def main(args: Array[String]): Unit = {
    //创建spark配置对象
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建上下文对象
    val sc: SparkContext = new SparkContext(config)

    val rdd1: RDD[Int] = sc.parallelize(1 to 5)
    val rdd2: RDD[Int] = sc.parallelize(6 to 10)

    //对两个RDD求并集返回一个新的RDD
    val unionrdd: RDD[Int] = rdd1.union(rdd2)

    unionrdd.collect().foreach(println)
  }
}
