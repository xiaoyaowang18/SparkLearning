package com.whc.bigdata.spark.operator.keyvalue类型

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 按照key进行聚合，在shuffle之前有combine（预聚合），返回结果是RDD[K,V]
//比groupbykey效率更高，shuffle阶段操作的数据更少
object ReduceByKkey {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ReduceByKey")

    val sc = new SparkContext(sparkConf)

    val words = Array("one", "two", "two", "three", "three", "three")
    val wordpairrdd: RDD[(String, Int)] = sc.parallelize(words).map(word => (word, 1))

    val rc: RDD[(String, Int)] = wordpairrdd.reduceByKey(_+_)

    rc.collect().foreach(println)
  }
}

