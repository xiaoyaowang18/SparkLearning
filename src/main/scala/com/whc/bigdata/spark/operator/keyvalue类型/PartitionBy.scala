package com.whc.bigdata.spark.operator.keyvalue类型

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

//对pairRDD进行分区操作，如果原有的partionRDD和现有的partionRDD是一致的话就不进行分区， 否则会生成ShuffleRDD，即会产生shuffle过程。


object PartitionBy {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Partitionby")

    val sc = new SparkContext(config)

    val rdd: RDD[(Int, String)] = sc.parallelize(Array((1, "aa"), (2, "bb"), (3, "cc"), (4, "dd")), 4)

    //对rdd重新分区
    //val rdd2: RDD[(Int, String)] = rdd.partitionBy(new org.apache.spark.HashPartitioner(2))

    //分区数为2
    //println(rdd2.partitions.size)
    val myrdd: RDD[(Int, String)] = rdd.partitionBy(new MyPartitioner(2))

    myrdd.saveAsTextFile("output")

  }
}

//自定义分区器
class MyPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = {
    partitions
  }

  //分配分区的逻辑
  override def getPartition(key: Any): Int = {
    1
  }
}

