package com.whc.bigdata.spark.SparkCore.operator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object MapPartitionsRdd {
  def main(args: Array[String]): Unit = {
    //创建spark配置对象
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建上下文对象
    val sc: SparkContext = new SparkContext(config)

    val listrdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    //类似于map，但独立地在RDD的每个分片上运行，因此在类型为T的RDD上运行时，func的函数类型必须是Iterator[T] => Iterator[U]
    //问题：每次处理一个分区的数据，这个分区的数据处理完后，原RDD中分区的数据才能释放，可能导致OOM。
    //建议内存空间较大的时候使用mappartition，以提高效率。
    val mappartitionsrdd: RDD[Int] = listrdd.mapPartitions(x=>x.map(_*2))

    mappartitionsrdd.collect().foreach(println)

  }
}
