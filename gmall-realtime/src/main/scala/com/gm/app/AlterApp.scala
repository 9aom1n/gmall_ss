package com.gm.app

/**
 * @Author: Gm
 * @Date: 2021/8/3 9:04
 */

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.gm.bean.{CouponAlertInfo, EventLog}
import com.gm.constants.GmallConstants
import com.gm.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._
object AlterApp {
  def main(args: Array[String]): Unit = {
    //创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlterApp");

    //创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //消费kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVRNT, ssc)

    //将数据转化成样例类（EventLog），补充时间字段将数据转换成（k,v） k->mid v->log
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val midToEventLogDStream: DStream[(String, EventLog)] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
        val times: String = sdf.format(new Date(eventLog.ts))
        //补全字段
        eventLog.logDate = times.split(" ")(0)
        eventLog.logHour = times.split(" ")(1)
        (eventLog.mid, eventLog)
      })
    })

    //开窗 5min
    val midToEventLogWindowDStream: DStream[(String, EventLog)] = midToEventLogDStream.window(Minutes(5))

    //对相同的mid 进行分组 groupByKey
    val midToIterEventLogDStream: DStream[(String, Iterable[EventLog])] = midToEventLogWindowDStream.groupByKey()

    //筛选数据，首先用户得领优惠券，并且没有浏览商品行为（将符合这些的uid保存下来至set集合）
    val boolToAlterDStream: DStream[(Boolean, CouponAlertInfo)] = midToIterEventLogDStream.mapPartitions(partition => {
      partition.map {
        case (mid, iter) =>
          //创建set集合用来保存没有浏览商品但是领取优惠券的id
          val uids: util.HashSet[String] = new util.HashSet[String]()
          //创建set集合用来保存领取优惠券所涉及的商品id
          val itemIds: util.HashSet[String] = new util.HashSet[String]()
          //创建list集合用来保存用户所涉及的行为
          val events: util.ArrayList[String] = new util.ArrayList[String]()

          //定义标志位用来判断用户是否存在浏览商品的行为
          var bool: Boolean = true

          //遍历迭代器中的每一条数据
          breakable {
            iter.foreach(log => {
              //向集合中添加所涉及的行为
              events.add(log.evid)
              if ("clickItem".equals(log.evid)) {
                //有浏览商品
                bool = false
                break()
              } else if ("coupon".equals(log.evid)) {
                //没有浏览商品
                uids.add(log.uid)
                itemIds.add(log.itemid)
              }
            })
          }
          //生成疑似预警日志
          (uids.size() >= 3 && bool, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
      }
    })
    boolToAlterDStream



    //筛选符合预警需求的数据
    val couponAlterInfoDStream: DStream[CouponAlertInfo] = boolToAlterDStream.filter(_._1).map(_._2)
    couponAlterInfoDStream.print()

    //将预警日志写入ES
    couponAlterInfoDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val list: List[(String, CouponAlertInfo)] = partition.toList.map(log => {
          (log.mid + log.ts / 1000 / 60, log)
        })
        MyEsUtil.insertBulk(GmallConstants.ALERT_INDEX_NAME_PREFIXES,list)
      })
    })

    //开启任务，并阻塞
    ssc.start()
    ssc.awaitTermination()
  }
}
