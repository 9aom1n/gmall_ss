package com.gm.app

import com.gm.constants.GmallConstants
import com.gm.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: Gm
 * @Date: 2021/7/29 20:04
 */

object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    val startUpStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)
    
  }
}
