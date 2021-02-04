package com.whc.bigdata.spark.SparkCore.operator.keyvalue类型

import org.apache.spark.{SparkConf, SparkContext}

//创建两个pairRDD，并将key相同的数据聚合到一个元组。
object Join {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Join")

    val sc = new SparkContext(sparkConf)

    val rdd = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c")))

    val rdd1 = sc.parallelize(Array((1, 4), (2, 5), (3, 6)))

    rdd.join(rdd1).collect().foreach(println)
  }
}
