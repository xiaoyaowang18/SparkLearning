package com.whc.bigdata.spark.operator.keyvalue类型

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

//创建一个pairRDD，将相同key对应值聚合到一个sequence中，并计算相同key对应值的相加结果。


object GroupByKey {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Partitionby")

    val sc = new SparkContext(config)

    val words = Array("one", "two", "two", "three", "three", "three")
    val wordpairrdd: RDD[(String, Int)] = sc.parallelize(words).map(word => (word, 1))

    val group: RDD[(String, Iterable[Int])] = wordpairrdd.groupByKey()

    val res: RDD[(String, Int)] = group.map(t => (t._1, t._2.sum))

    res.collect().foreach(println)
  }
}
