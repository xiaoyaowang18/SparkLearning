package com.whc.bigdata.spark.operator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object GroupByRdd {
  def main(args: Array[String]): Unit = {
    //创建spark配置对象
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建上下文对象
    val sc: SparkContext = new SparkContext(config)

    val listrdd: RDD[Int] = sc.makeRDD(1 to 4)

    //groupBy会按照传入的函数的返回值进行分组
    //分组后的数据形成对偶元组（K-V） K代表分组后的key  V表示分组后的数据集合
    val groupbyrdd: RDD[(Int, Iterable[Int])] = listrdd.groupBy(i => i % 2)

    groupbyrdd.collect().foreach(println)
  }
}
