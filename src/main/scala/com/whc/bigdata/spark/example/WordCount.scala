package com.whc.bigdata.spark.example

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {

    //创建sparkConf对象
    //设定spark计算框架的运行（部署）环境  及appid
    val conf = new SparkConf().setMaster("local[*]").setAppName("wordcount")

    //创建spark上下文对象
    val sc = new SparkContext(conf)

    //读取文件，将文件内容一行一行打印出来
    val lines = sc.textFile("in/1.txt")

    //将一行行文字内容分解成一个个单词
    val words = lines.flatMap(_.split(" "))

    //为了统计方便，将单词数据进行结构的转换,k-v的形式
    val wordtoone = words.map((_,1))

    //对转换后的数据进行分组聚合
    val wordtosum = wordtoone.reduceByKey(_+_)

    val result = wordtosum.collect()

    result.foreach(println)

  }
}
