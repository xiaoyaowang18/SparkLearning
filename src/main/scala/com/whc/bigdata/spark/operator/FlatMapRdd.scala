package com.whc.bigdata.spark.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FlatMapRdd {
  def main(args: Array[String]) = {

    //创建spark配置对象
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建上下文对象
    val sc: SparkContext = new SparkContext(config)

    val listrdd: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2), List(3, 4)))

    //flatmap
    //输出1234
    val flatmaprdd: RDD[Int] = listrdd.flatMap(datas => datas)

    flatmaprdd.collect().foreach(println)

  }
}
