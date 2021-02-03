package com.whc.bigdata.spark.operator.Action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SaveAs {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaveAs")

    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.parallelize(Array(1, 3, 2, 4, 5))

    rdd.saveAsTextFile("output1")
    rdd.saveAsObjectFile("output2")

  }
}
