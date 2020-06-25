package com.qzy.spark.streaming.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author qzy1773498686@qq.com
  * @since 2019/6/25 5:30 PM
  */
object StreamingWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")
    val sc = new StreamingContext(conf, Seconds(1))
    sc.sparkContext.setLogLevel("OFF")

    val lines = sc.socketTextStream("127.0.0.1", 9999)
    val words = lines.flatMap(_.split("\\s+"))
    val count = words.map((_, 1)).reduceByKey(_ + _)
    count.print()

    sc.start()
    sc.awaitTermination()
  }
}
