package com.whc.bigdata.spark.operator.keyvalue类型

import org.apache.spark.{SparkConf, SparkContext}

//创建两个pairRDD，并将key相同的数据聚合到一个迭代器。
object CoGroup {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AggregateByKey")

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c")))

    val rdd1 = sc.parallelize(Array((1, 4), (2, 5), (3, 6)))

    //(1,(CompactBuffer(a),CompactBuffer(4)))
    //(2,(CompactBuffer(b),CompactBuffer(5)))
    //(3,(CompactBuffer(c),CompactBuffer(6)))
    rdd.cogroup(rdd1).collect().foreach(println)
  }
}
