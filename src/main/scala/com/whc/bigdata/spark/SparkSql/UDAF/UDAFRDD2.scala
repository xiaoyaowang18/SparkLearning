package com.whc.bigdata.spark.SparkSql.UDAF

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession, TypedColumn}

object UDAFRDD2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UDF")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("D:\\ideaproject\\SparkLearning\\src\\main\\scala\\com\\whc\\bigdata\\resources\\user.json")

    //创建聚合函数
    val udaf = new MyAvgClassFunction

    //将聚合函数转换为查询列
    val avgCol: TypedColumn[UserBean, Double] = udaf.toColumn.name("avgAge")

    val userDS: Dataset[UserBean] = df.as[UserBean]

    userDS.select(avgCol).show()


    //释放资源
    spark.stop()


  }
}

case class UserBean(name: String, age: BigInt)

case class AvgBuffer(var sum: BigInt, var count: Int)

//申明用户自定义聚合函数（强类型）
//1.继承Aggregator,设定泛型
//2.实现方法
class MyAvgClassFunction extends Aggregator[UserBean,AvgBuffer,Double] {
  //初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0,0)
  }

  //聚合数据
  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
    b.sum = b.sum +a.sum
    b.count = b.count+1
    b
  }

  //缓冲区合并操作
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  //完成计算
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble/reduction.count
  }

  //如果是自定义的类型
  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}