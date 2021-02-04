package com.whc.bigdata.spark.SparkCore.operator.双value类型

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ZipRdd {
  def main(args: Array[String]): Unit = {
    //创建spark配置对象
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建上下文对象
    val sc: SparkContext = new SparkContext(config)

    val rdd1: RDD[Int] = sc.parallelize(Array(1,2,3),3)
    val rdd2: RDD[String] = sc.parallelize(Array("a","b","c"),3)

    //将两个RDD组合成Key/Value形式的RDD,这里默认两个RDD的partition数量以及元素数量都相同，否则会抛出异常。
    val ziprdd: RDD[(Int, String)] = rdd1.zip(rdd2)

    ziprdd.collect().foreach(println)
  }
}
