package com.whc.bigdata.spark.SparkSql.数据源

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Spark SQL可以通过JDBC从关系型数据库中读取数据的方式创建DataFrame，通过对DataFrame一系列的计算后，还可以将数据再写回关系型数据库中。
 */
object JDBC {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("readload").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //从mysql数据库加载数据方式一
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/rdd")
      .option("dbtable", "rddtable")
      .option("user", "root")
      .option("password", "000000")
      .load()

    //从mysql数据库加载数据方式二
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "000000")
    val jdbcDF2 = spark.read
      .jdbc("jdbc:mysql://hadoop102:3306/rdd", "rddtable", connectionProperties)

    //将数据写入mysql方式一
    jdbcDF.write.
      .format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/rdd")
      .option("dbtable", "dftable")
      .option("user", "root")
      .option("password", "000000")
      .save()

    //将数据写入mysql方式二
    jdbcDF2.write
      .jdbc("jdbc:mysql://hadoop102:3306/rdd", "db", connectionProperties)


  }
}
