package com.whc.bigdata.spark.累加器与广播变量

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 累加器用来对信息进行聚合，通常在向 Spark传递函数时，比如使用 map() 函数或者用 filter() 传条件时，
 * 可以使用驱动器程序中定义的变量，但是集群中运行的每个任务都会得到这些变量的一份新的副本，
 * 更新这些副本的值也不会影响驱动器中的对应变量。如果我们想实现所有分片处理时更新共享变量的功能，
 * 那么累加器可以实现我们想要的效果。
 */
object AccumulatorRDD {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JDBCRDD")
    //2.创建sparkcontext
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.parallelize(List("hadoop","hive","spark","es"),2)

    //求和 使用reduce 分区内数据传输到excutor中，分区内先相加，再分区间相加，效率其实不高、
    //val i: Int = rdd.reduce(_+_)

    //使用累加器来共享变量，累加数据
    //创建累加器对象
    /*val accumulator: LongAccumulator = sc.longAccumulator

    rdd.foreach{
      case i => {
        //执行累加器的累加功能
        accumulator.add(i)
      }
    }

    //获取累加器的值
    print(accumulator.value)*/

    // TODO 创建累加器
    val wordAccumulator = new WordAccumulator()
    // TODO 注册累加器
    sc.register(wordAccumulator)

    rdd.foreach{
      case word => {
        wordAccumulator.add(word)
      }
    }

    // 获取累加器的值
    println("sum: "+wordAccumulator.value)
  }
}

//自定义累加器
/**
 * 1.继承AccumulatorV2
 * 2.实现抽象方法
 */
class WordAccumulator extends AccumulatorV2[String,util.ArrayList[String]]{

  var list = new util.ArrayList[String]()

  //当前的累加器是否为初始状态
  override def isZero: Boolean = list.isEmpty

  //复制累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new WordAccumulator()
  }

  //重置累加器对象
  override def reset(): Unit = {
    list.clear()
  }

  //向累加器中增加数据
  override def add(v: String): Unit = {
    if (v.contains("h")){
        list.add(v)
    }
  }

  //合并
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
      list.addAll(other.value)
  }

  //获取累加器的结果
  override def value: util.ArrayList[String] = list
}