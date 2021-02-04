package com.whc.bigdata.spark.SparkCore.operator.keyvalue类型

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//aggregateByKey的简化操作，查看foldBykey的源码，发现seqop和combop相同
object FoldByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ReduceByKey")

    val sc = new SparkContext(sparkConf)

    val pairrdd: RDD[(Int, Int)] = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),2)

    val fdd: RDD[(Int, Int)] = pairrdd.foldByKey(0)(_+_)

    fdd.collect().foreach(println)
  }
}
