package com.whc.bigdata.spark.SparkCore.operator.keyvalue类型

import org.apache.spark.{SparkConf, SparkContext}

//需求：创建一个pairRDD，按照key的正序和倒序进行排序

object SortByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SortByKey")

    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(Array((3, "aa"), (6, "cc"), (2, "bb"), (1, "dd")), 2)

    //正序
    rdd.sortByKey(true).collect().foreach(println)
    //逆序
    rdd.sortByKey(true).collect().foreach(println)
  }
}
