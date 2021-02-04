package com.whc.bigdata.spark.SparkCore.数据读取与保存.文件系统

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object MysqlRDD {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JDBCRDD")
    //2.创建sparkcontext
    val sc = new SparkContext(conf)

    //3.定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"

    val url = "jdbc:mysql://bigdata1.whc.com:3306/rdd"

    val userName = "root"

    val passWd = "123456"

    //创建jdbcrdd
    //查询数据
    /*val jdbcrdd = new JdbcRDD(sc, () => {
      Class.forName(driver)
      DriverManager.getConnection(url, username, passwd)
    },
      "select * from rddtable where id >= ? and id <= ?",
      1,
      10,
      1,
      r => (r.getString(2), r.getInt(3))
    )
    jdbcrdd.foreach(println)*/
    //保存数据
    val datardd: RDD[(String, Int)] = sc.makeRDD(List(("whc", 18), ("www", 22), ("hhh", 11)))
    //这种方式有多少数据，就会创建多少连接，效率不高，不推荐
    /*datardd.foreach {
      case (username, age) => {
        Class.forName(driver)
        val conn: Connection = DriverManager.getConnection(url, userName, passWd)
        val sql = "insert into rddtable(username,age) values (?,?)"
        val statement: PreparedStatement = conn.prepareStatement(sql)
        statement.setString(1, username)
        statement.setInt(2, age)
        statement.executeUpdate()
        statement.close()
        conn.close()
      }
    }*/
    //这种方式有多少分区，就会创建多少连接，比上面的效率高，但是有OOM风险
    datardd.foreachPartition(datas => {
      Class.forName(driver)
      val conn: Connection = DriverManager.getConnection(url, userName, passWd)
      datas.foreach {
        case (username, age) => {
          val sql = "insert into rddtable(username,age) values (?,?)"
          val statement: PreparedStatement = conn.prepareStatement(sql)
          statement.setString(1, username)
          statement.setInt(2, age)
          statement.executeUpdate()
          statement.close()
        }
      }
      conn.close()
    })
    sc.stop()

  }


}
