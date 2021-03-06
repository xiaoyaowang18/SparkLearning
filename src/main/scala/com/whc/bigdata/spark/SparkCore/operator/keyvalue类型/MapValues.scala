package com.whc.bigdata.spark.SparkCore.operator.keyvalue类型

import org.apache.spark.{SparkConf, SparkContext}

object MapValues {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MapValues")

    val sc = new SparkContext(sparkConf)

    val rdd = sc.parallelize(Array((1,"a"),(1,"d"),(2,"b"),(3,"c")))

    //对value添加字符
    rdd.mapValues(_+"哈哈哈").collect().foreach(println)
  }
}
