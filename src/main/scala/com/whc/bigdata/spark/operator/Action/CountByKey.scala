package com.whc.bigdata.spark.operator.Action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 1. 作用：针对(K,V)类型的RDD，返回一个(K,Int)的map，表示每一个key对应的元素个数。
 * 2. 需求：创建一个PairRDD，统计每种key的个数
 */
object CountByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CountByKey")

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)), 3)

    val intToLong: collection.Map[Int, Long] = rdd.countByKey()

    println(intToLong)
  }
}
