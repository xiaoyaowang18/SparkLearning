package com.whc.bigdata.spark.SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

object StreamKafkaSource {
  def main(args: Array[String]): Unit = {
    //1.初始化spark配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamingWordCount")

    //2.初始化sparkstreamingcontext。以指定的时间为周期采集实时数据
    val streamingContext = new StreamingContext(conf,Seconds(3))

    //3.从kafka中采集数据
    val kafkaDstream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(streamingContext,
      "bigdata1:2181","sparkstreaming",Map("sparkstream"->3))

    //4.将每一行数据做切分，形成一个个单词  传入tuple  kafka传入k-v对，k默认为null,我们需要的是v
    val wordStreams: DStream[String] = kafkaDstream.flatMap(t=>t._2.split(" "))

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
