package com.whc.bigdata.spark.SparkCore.operator.value类型

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SortByRdd {
  def main(args: Array[String]): Unit = {
    //创建spark配置对象
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建上下文对象
    val sc: SparkContext = new SparkContext(config)

    val listrdd: RDD[Int] = sc.makeRDD(List(1,4,3,2))

    // 有三个参数：f: (T) => K,ascending: Boolean = true,numPartitions: Int = this.partitions.length
    // 默认是正序排序，倒叙排序可以加入参数ascending
    //还可以指定分区数
    val sortbyrdd: RDD[Int] = listrdd.sortBy(x=>x,ascending=false)

    sortbyrdd.collect().foreach(println)


  }
}
