package com.whc.bigdata.spark.operator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SampleRdd {
  def main(args: Array[String]): Unit = {
    //创建spark配置对象
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建上下文对象
    val sc: SparkContext = new SparkContext(config)

    val listrdd: RDD[Int] = sc.makeRDD(1 to 16)

    //以指定的随机种子随机抽样出数量为fraction的数据，withReplacement表示是抽出的数据是否放回，true为有放回的抽样，false为无放回的抽样，seed用于指定随机数生成器种子。
    //对数据源进行多次抽样，可以查看哪个数据出现的次数多，可能造成数据倾斜。
    val samplerdd: RDD[Int] = listrdd.sample(true,2,2)

    samplerdd.collect().foreach(println)
  }
}
