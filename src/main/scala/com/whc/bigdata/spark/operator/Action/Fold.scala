package com.whc.bigdata.spark.operator.Action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * 1. 作用：折叠操作，aggregate的简化操作，seqop和combop一样。
 *
 * 当分区内和分区间操作相同时，可以用fold
 *
 * 2. 需求：创建一个RDD，将所有元素相加得到结果
 */
object Fold {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Fold")

    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(1 to 10,2)

    println(rdd.fold(0)(_ + _))

  }
}
