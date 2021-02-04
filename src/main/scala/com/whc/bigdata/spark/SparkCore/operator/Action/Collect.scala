package com.whc.bigdata.spark.SparkCore.operator.Action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * 1. 作用：在驱动程序中，以数组的形式返回数据集的所有元素。
 * 2. 需求：创建一个RDD，并将RDD内容收集到Driver端打印
 */

object Collect {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Collect")

    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(1 to 10,2)

    rdd.collect()

  }
}
