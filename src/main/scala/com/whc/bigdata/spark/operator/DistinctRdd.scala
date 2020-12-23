package com.whc.bigdata.spark.operator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object DistinctRdd {
  def main(args: Array[String]): Unit = {
    //创建spark配置对象
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建上下文对象
    val sc: SparkContext = new SparkContext(config)

    val listrdd: RDD[Int] = sc.makeRDD(List(1,2,5,1,2,9,6,8,3,4,7))

    //使用distinct算子对数据去重，因为去重后会导致数据减少，所以可以改变默认的分区数量。默认的话是8个分区
    //val distinctrdd: RDD[Int] = listrdd.distinct()
    //会将数据打乱重组，从而产生shuffle操作，运行较慢。
    val distinctrdd: RDD[Int] = listrdd.distinct(2)

    //distinctrdd.collect().foreach(println)

    distinctrdd.saveAsTextFile("out")

  }
}
