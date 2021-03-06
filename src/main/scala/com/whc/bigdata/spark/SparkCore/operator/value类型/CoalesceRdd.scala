package com.whc.bigdata.spark.SparkCore.operator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CoalesceRdd {
  def main(args: Array[String]): Unit = {
    //创建spark配置对象
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建上下文对象
    val sc: SparkContext = new SparkContext(config)

    val listrdd: RDD[Int] = sc.makeRDD(1 to 16,4)

    println("缩减分区前："+listrdd.partitions.size)

    //缩减分区：也可以理解为合并分区，可以查看coalescerdd的输出output。
    //查看源码，发现可以选择是否进行shuffle过程，由参数shuffle: Boolean = false/true决定。
    val coalescerdd: RDD[Int] = listrdd.coalesce(3)

    println("缩减分区后："+coalescerdd.partitions.size)

    coalescerdd.saveAsTextFile("output")
  }
}
