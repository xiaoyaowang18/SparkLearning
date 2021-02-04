package com.whc.bigdata.spark.SparkCore.operator.keyvalue类型

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 数据结构：时间戳，省份，城市，用户，广告，中间字段使用空格分割。
 *
 * 需求：统计出每一个省份广告被点击次数的TOP3
 */


object Practice {
  def main(args: Array[String]): Unit = {
    //1.初始化spark配置信息并建立与spark的连接
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Practice")
    val sc = new SparkContext(sparkConf)

    //2.读取数据生成RDD：TS，Province，City，User，AD
    val line: RDD[String] = sc.textFile("D:\\ideaproject\\SparkLearning\\src\\main\\scala\\com\\whc\\bigdata\\resources\\agent.log")

    //3.按照最小粒度聚合：((Province,AD),1)
    val provinceAdToOne: RDD[((String, String), Int)] = line.map { x =>
      val fields: Array[String] = x.split(" ")
      ((fields(1), fields(4)), 1)
    }

    //4.计算每个省中每个广告被点击的次数：((Province,AD),sum)
    val provinceAdToSum: RDD[((String, String), Int)] = provinceAdToOne.reduceByKey(_+_)

    //5.将省份作为key，广告点击数作为value：(Province,(AD,sum))
    val provinceToAdSum : RDD[(String, (String, Int))] = provinceAdToSum.map(x=>(x._1._1,(x._1._2,x._2)))

    //6.将同一个省份的所有广告进行聚合(Province,List((AD1,sum1),(AD2,sum2)...))
    val provinceGroup: RDD[(String, Iterable[(String, Int)])] = provinceToAdSum.groupByKey()

    //7.对同一个省份所有广告的集合进行排序并取前3条，排序规则为广告点击总数
    val provinceAdTop3: RDD[(String, List[(String, Int)])] = provinceGroup.mapValues { x =>
      x.toList.sortWith((x, y) => x._2 > y._2).take(3)
    }

    //8.数据拉取到Driver端打印
    provinceAdTop3.collect().foreach(println)

    //9.关闭与spark的连接
    sc.stop()

  }
}
