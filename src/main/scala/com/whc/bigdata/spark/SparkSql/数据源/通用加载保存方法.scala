package com.whc.bigdata.spark.SparkSql.数据源

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object 通用加载保存方法 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("readload").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    /**
     * Spark SQL的默认数据源为Parquet格式。数据源为Parquet文件时，Spark SQL可以方便的执行所有的操作。
     * 修改配置项spark.sql.sources.default，可修改默认数据源格式。
     */
    val df = spark.read.load("examples/src/main/resources/users.parquet")
    df.select("name", "favorite_color").write.save("namesAndFavColors.parquet")

    /**
     * 当数据源格式不是parquet格式文件时，需要手动指定数据源的格式。数据源格式需要指定全名（例如：org.apache.spark.sql.parquet），
     * 如果数据源格式为内置格式，则只需要指定简称定json, parquet, jdbc, orc, libsvm, csv, text来指定数据的格式。
     * 可以通过SparkSession提供的read.load方法用于通用加载数据，使用write和save保存数据
     */
    val peopleDF = spark.read.format("json").load("examples/src/main/resources/people.json")
    peopleDF.write.format("parquet").mode("append").save("hdfs://hadoop102:9000/namesAndAges.parquet")

    //还可以直接运行sql在文件上
    val sqlDF = spark.sql("SELECT * FROM parquet.`hdfs://hadoop102:9000/namesAndAges.parquet`")
    sqlDF.show()

    //可以采用SaveMode执行存储操作，SaveMode定义了对数据的处理模式。
    //error(default)  append  overwrite  ignore

  }
}
