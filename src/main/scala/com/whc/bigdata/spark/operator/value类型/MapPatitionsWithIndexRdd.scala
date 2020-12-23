package com.whc.bigdata.spark.operator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object MapPatitionsWithIndexRdd {
  def main(args: Array[String]): Unit = {
    //创建spark配置对象
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建上下文对象
    val sc: SparkContext = new SparkContext(config)

    val listrdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    //类似于mapPartitions，但func带有一个整数参数表示分片的索引值，因此在类型为T的RDD上运行时，func的函数类型必须是(Int, Interator[T]) => Iterator[U]；
    //将list中的元素跟所在的分组形成一个元组
    val mappartitionswithindexrdd: RDD[(Int, Int)] = listrdd.mapPartitionsWithIndex((index,items)=>(items.map((index,_))))

    mappartitionswithindexrdd.collect().foreach(println)

  }
}
