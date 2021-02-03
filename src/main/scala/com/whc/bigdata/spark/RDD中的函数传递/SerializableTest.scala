package com.whc.bigdata.spark.RDD中的函数传递

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 在实际开发中我们往往需要自己定义一些对于RDD的操作，那么此时需要主要的是，
 * 初始化工作是在Driver端进行的，而实际运行程序是在Executor端进行的，这就涉及到了跨进程通信，是需要序列化的。
 */
object SerializableTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Serializable")

    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.parallelize(Array("hadoop","hive","spark"))

    //创建一个search对象
    val search = new Search("h")

    val rdd1: RDD[String] = search.getMatch1(rdd)

    rdd1.collect().foreach(println)

  }
}


class Search(query:String) extends Serializable {

  //过滤出包含字符串的数据
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  //过滤出包含字符串的RDD
  /**
   * 在这个方法中所调用的方法isMatch()是定义在Search这个类中的，实际上调用的是this. isMatch()
   * this表示Search这个类的对象，程序在运行过程中需要将Search对象序列化以后传递到Executor端。
   */
  def getMatch1 (rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }

  //过滤出包含字符串的RDD
  def getMatch2(rdd: RDD[String]): RDD[String] = {
    //字符串会自动序列化，可以不继承Serializable
    val q = query
    rdd.filter(x => x.contains(q))
  }

}
