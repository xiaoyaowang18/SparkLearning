package com.whc.bigdata.spark.operator.Action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 1. 参数：(zeroValue: U)(seqOp: (U, T) ⇒ U, combOp: (U, U) ⇒ U)
 * 2. 作用：aggregate函数将每个分区里面的元素通过seqOp和初始值进行聚合，
 * 然后用combine函数将每个分区的结果和初始值(zeroValue)进行combine操作。
 * 这个函数最终返回的类型不需要和RDD中元素类型一致。
 *
 * 注：分区间进行计算的时候也会加上初始值。
 */
object Aggregate {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Aggregate")

    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(1 to 10,2)

    val res: Int = rdd.aggregate(0)(_+_,_+_)

    print(res)
  }
}
