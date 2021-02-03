package com.whc.bigdata.spark.operator.Action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * 1. 作用：返回RDD中的第一个元素
 * 2. 需求：创建一个RDD，返回该RDD中的第一个元素
 */
object First {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("First")

    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(1 to 10, 2)

    print(rdd.first())

  }
}
