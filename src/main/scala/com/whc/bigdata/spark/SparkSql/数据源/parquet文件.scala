package com.whc.bigdata.spark.SparkSql.数据源

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Parquet是一种流行的列式存储格式，可以高效地存储具有嵌套字段的记录。Parquet格式经常在Hadoop生态圈中被使用，
 * 它也支持Spark SQL的全部数据类型。Spark SQL 提供了直接读取和存储 Parquet 格式文件的方法。
 */
object parquet文件 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("readload").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val peopleDF = spark.read.json("examples/src/main/resources/people.json")

    peopleDF.write.parquet("hdfs://hadoop102:9000/people.parquet")

    val parquetFileDF = spark.read.parquet("hdfs:// hadoop102:9000/people.parquet")

    parquetFileDF.createOrReplaceTempView("parquetFile")

    val namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19")
    namesDF.map(attributes => "Name: " + attributes(0)).show()

  }
}
