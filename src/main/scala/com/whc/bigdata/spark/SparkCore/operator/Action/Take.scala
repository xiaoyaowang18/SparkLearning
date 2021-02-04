package com.whc.bigdata.spark.SparkCore.operator.Action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 1. 作用：返回一个由RDD的前n个元素组成的数组
 * 2. 需求：创建一个RDD，统计该RDD的条数
 */
object Take {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Take")

    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.parallelize(Array(2,4,3,1,5))

    rdd.take(3).foreach(println)
  }
}
