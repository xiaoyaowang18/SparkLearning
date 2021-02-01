package com.whc.bigdata.spark.operator.keyvalue类型

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 参数：(zeroValue:U,[partitioner: Partitioner]) (seqOp: (U, V) => U,combOp: (U, U) => U)
 * 作用：在kv对的RDD中，，按key将value进行分组合并，合并时，将每个value和初始值作为seq函数的参数，进行计算，
 * 返回的结果作为一个新的kv对，然后再将结果按照key进行合并，最后将每个分组的value传递给combine函数进行计算（先将前两个value进行计算，
 * 将返回结果和下一个value传给combine函数，以此类推），将key与计算结果作为一个新的kv对输出。
 * （1）zeroValue：给每一个分区中的每一个key一个初始值；   0
 * （2）seqOp：函数用于在每一个分区中用初始值逐步迭代value；   math.max(_,_)
 * （3）combOp：函数用于合并每个分区中的结果。             _+_
 *
 * 需求：创建一个pairRDD,取出每个分区相同key对应值的最大值，然后相加
 */

object AggregateByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AggregateByKey")

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)

    //zerovalue : 0         初始值
    //seqOp: math.max(_,_)  分区内对每个key的值计算，获取最大值
    //combOp: _+_           将每个分区内的同一个key对应的值相加
    val agg: RDD[(String, Int)] = rdd.aggregateByKey(0)(math.max(_,_),_+_)

    agg.collect().foreach(println)
  }
}
