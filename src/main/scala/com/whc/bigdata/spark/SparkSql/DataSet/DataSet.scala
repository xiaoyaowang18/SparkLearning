package com.whc.bigdata.spark.SparkSql.DataSet

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * 1）是Dataframe API的一个扩展，是Spark最新的数据抽象。
 * 2）用户友好的API风格，既具有类型安全检查也具有Dataframe的查询优化特性。
 * 3）Dataset支持编解码器，当需要访问非堆上的数据时可以避免反序列化整个对象，提高了效率。
 * 4）样例类被用来在Dataset中定义数据的结构信息，样例类中每个属性的名称直接映射到DataSet中的字段名称。
 * 5）Dataframe是Dataset的特列，DataFrame=Dataset[Row] ，所以可以通过as方法将Dataframe转换为Dataset。Row是一个类型，跟Car、Person这些的类型一样，所有的表结构信息我都用Row来表示。
 * 6）DataSet是强类型的。比如可以有Dataset[Car]，Dataset[Person].
 * 7）DataFrame只是知道字段，但是不知道字段的类型，所以在执行这些操作的时候是没办法在编译的时候检查是否类型失败的，比如你可以对一个String进行减法操作，在执行的时候才报错，而DataSet不仅仅知道字段，而且知道字段类型，所以有更严格的错误检查。就跟JSON对象和类对象之间的类比。
 */
object DataSet {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DataSet")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "whc", 18), (2, "hhh", 18)))

    //进入转换前，需要引入隐式转换规则
    //这里的spark不是包名的含义，是SparkSession对象的名字
    import spark.implicits._
    //转换为DF
    val df: DataFrame = rdd.toDF("id", "name", "age")

    //转换为DS
    val ds: Dataset[user] = df.as[user]

    //转换为df
    val df1: DataFrame = ds.toDF()

    //转换为rdd
    val rdd1: RDD[Row] = df1.rdd

    rdd1.foreach(row => {
      //获取数据时，可以通过索引访问数据
      row.getString(1)
    })

    //RDD -> DataSet  RDD直接换砖成DataSet
    val userrdd: RDD[user] = rdd.map {
      case (id, name, age) => {
        user(id, name, age)
      }
    }
    val userDataSet: Dataset[user] = userrdd.toDS()

    userDataSet.foreach(user => {
      println(user)
    })

  }
}

//样例类
case class user(id: Int, name: String, age: Int)