package com.whc.bigdata.spark.SparkCore.operator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object MapRdd {
  def main(args: Array[String]): Unit = {
    //创建spark配置对象
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建上下文对象
    val sc: SparkContext = new SparkContext(config)

    val listrdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    //map:返回一个新的RDD，该RDD由每一个输入元素经过func函数转换后组成
    val maprdd: RDD[Int] = listrdd.map(_*2)

    maprdd.collect().foreach(println)

  }
}
