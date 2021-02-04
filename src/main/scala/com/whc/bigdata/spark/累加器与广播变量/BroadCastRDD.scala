package com.whc.bigdata.spark.累加器与广播变量

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 广播变量用来高效分发较大的对象。向所有工作节点发送一个较大的只读值，以供一个或多个Spark操作使用。
 * 比如，如果你的应用需要向所有节点发送一个较大的只读查询表，甚至是机器学习算法中的一个很大的特征向量，
 * 广播变量用起来都很顺手。 在多个并行操作中使用同一个变量，但是 Spark会为每个任务分别发送。
 */
object BroadCastRDD {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JDBCRDD")
    //2.创建sparkcontext
    val sc = new SparkContext(conf)

    val rdd1: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (2, "b"), (3, "c")))

    //val rdd2: RDD[(Int, Int)] = sc.makeRDD(List((1, 2), (2, 2), (3, 3)))
    //rdd1.join(rdd2)  join效率低  可以用广播变量优化

    var list = List((1, 2), (2, 2), (3, 3))

    //可以使用广播变量减少数据的传输
    val broadcast: Broadcast[List[(Int, Int)]] = sc.broadcast(list)

    val resultRdd: RDD[(Int, (String, Any))] = rdd1.map {
      case (key, value) => {
        var v2: Any = null
        for (t <- broadcast.value) {
          if (key == t._1) {
            v2 = t._2
          }
        }
        (key, (value, v2))
      }
    }

    resultRdd.foreach(println)
  }
}
