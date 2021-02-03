package com.whc.bigdata.spark.operator.Action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 1. 作用：通过func函数聚集RDD中的所有元素，先聚合分区内数据，再聚合分区间数据。
 * 2. 需求：创建一个RDD，将所有元素聚合得到结果。
 */
object Reduce {
  def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Reduce")

        val sc = new SparkContext(conf)

        val rdd: RDD[Int] = sc.makeRDD(1 to 10,2)

        val res: Int = rdd.reduce(_+_)

        print(res)
  }
}
