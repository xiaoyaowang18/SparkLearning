package com.whc.bigdata.spark.SparkCore.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GlomRdd {
  def main(args: Array[String]): Unit = {
    //创建spark配置对象
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建上下文对象
    val sc: SparkContext = new SparkContext(config)

    val listrdd: RDD[Int] = sc.makeRDD(1 to 16,3)

    //将一个分区的数据放到一个数组中,如果有需求是从分区中统计数据的，可以用这个rdd
    val glomrdd: RDD[Array[Int]] = listrdd.glom()

    glomrdd.collect().foreach(array=>{
      println(array.mkString(","))
    })

  }
}
