package com.whc.bigdata.spark.SparkCore.operator.Action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 1. 作用：在数据集的每一个元素上，运行函数func进行更新。
 * 2. 需求：创建一个RDD，对每个元素进行打印
 */
object Foreach {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Foreach")

    val sc = new SparkContext(conf)

    var rdd = sc.makeRDD(1 to 5, 2)

    //这行代码会在driver中执行
    rdd.foreach(println(_))
  }
}
