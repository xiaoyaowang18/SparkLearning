package com.whc.bigdata.spark.SparkCore.operator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RepartitionRdd {
  def main(args: Array[String]): Unit = {
    //创建spark配置对象
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建上下文对象
    val sc: SparkContext = new SparkContext(config)

    val listrdd: RDD[Int] = sc.makeRDD(1 to 16,4)

    //根据传入的分区数，重新洗牌所有数据，故有shuffle过程
    //查看源码，发现它调用的是coalesce方法，传入参数shuffle = true
    val repartitionrdd: RDD[Int] = listrdd.repartition(2)

    println(repartitionrdd.partitions.size)
  }
}
