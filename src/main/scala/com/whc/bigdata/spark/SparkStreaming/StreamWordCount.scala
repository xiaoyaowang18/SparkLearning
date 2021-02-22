package com.whc.bigdata.spark.SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamWordCount {
  def main(args: Array[String]): Unit = {
      //1.初始化spark配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamingWordCount")

    //2.初始化sparkstreamingcontext
    val streamingContext = new StreamingContext(conf,Seconds(3))

    //3.通过监控端口创建Dstream,读进来的数据为一行行
    val lineStreams: ReceiverInputDStream[String] = streamingContext.socketTextStream("bigdata1.whc.com",8888)

    //4.将每一行数据做切分，形成一个个单词
    val wordStreams: DStream[String] = lineStreams.flatMap(_.split(" "))

    //5.将单词映射成元组 (word,1)
    val wordToOneStreams: DStream[(String, Int)] = wordStreams.map((_,1))

    //6.将相同的单词次数做统计
    val wordCountStreams: DStream[(String, Int)] = wordToOneStreams.reduceByKey(_+_)

    //7.打印
    wordCountStreams.print()

    //启动采集器
    streamingContext.start()
    //driver等待采集器的执行
    streamingContext.awaitTermination()
  }
}
