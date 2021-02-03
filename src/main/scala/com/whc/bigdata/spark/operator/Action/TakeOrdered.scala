package com.whc.bigdata.spark.operator.Action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 1. 作用：返回该RDD排序后的前n个元素组成的数组
 * 2. 需求：创建一个RDD，统计该RDD的条数
 */

object TakeOrdered {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TakeOrdered")

    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.parallelize(Array(2,4,1,3,5))

    rdd.takeOrdered(3).foreach(println)
  }
}
