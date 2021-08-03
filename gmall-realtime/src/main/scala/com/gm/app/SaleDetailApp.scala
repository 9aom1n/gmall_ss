package com.gm.app

import java.util

import com.alibaba.fastjson.JSON
import com.gm.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.gm.constants.GmallConstants
import com.gm.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.Serialization
import redis.clients.jedis.Jedis

import collection.JavaConverters._

/**
 * @Author: Gm
 * @Date: 2021/8/3 18:32
 */

object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    //TODO 1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")
    //TODO 2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    //TODO 3.分别获取kafka中order_info 和 order_detail数据
    val oederInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)

    //TODO 4.将数据转为样例类，并转为kv结构，为了后面join是操作
    val orderInfoDStream: DStream[(String, OrderInfo)] = oederInfoKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        //补全字段
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)

        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
        (orderInfo.id, orderInfo)
      })
    })

    val detailDStream: DStream[(String, OrderDetail)] = orderDetailKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
        (orderDetail.id, orderDetail)
      })
    })

    //TODO 5.将两条流的数据join起来
    val orderIdToInfoOptWithDetailOptDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDStream.fullOuterJoin(detailDStream)

    //TODO 6.使用缓存的方式将数据关联起来
    val noUserSaleDetailDStream: DStream[SaleDetail] = orderIdToInfoOptWithDetailOptDStream.mapPartitions(partition => {
      implicit val formats = org.json4s.DefaultFormats
      //创建集合用来存放关联起来的结果数据
      val details: util.ArrayList[SaleDetail] = new util.ArrayList[SaleDetail]()
      //创建redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)

      partition.foreach { case (orderId, (infoOpt, detailOpt)) =>
        //存放orderInfo的redisKey
        val infoRedisKey: String = "OrderInfo:" + orderId
        //存放orderDetail的RedisKey
        val detailRedisKey: String = s"OrderDetail:${orderId}"

        //todo 1.判断orderInfo是否存在
        if (infoOpt.isDefined) {
          //orderInfo数据存在
          val orderInfo: OrderInfo = infoOpt.get

          //todo 2.判断orderDetail是否存在
          if (infoOpt.isDefined) {
            //orderDetail数据存在
            val orderDetail: OrderDetail = detailOpt.get

            //todo 3.将两个数据关联起来
            val saleDetail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
            details.add(saleDetail)
          }
          //todo 4.去对方（orderDetail）缓存中查询是否有能关联上的数据
          //判断redis的key在redis中是否存在
          if (jedis.exists(detailRedisKey)) {
            val orderDetailJsomSet: util.Set[String] = jedis.smembers(detailRedisKey)
            //遍历set集合获取到每个orderDetail的数据
            for (elem <- orderDetailJsomSet.asScala) {
              val orderDetail: OrderDetail = JSON.parseObject(elem, classOf[OrderDetail])
              val saleDetail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
              details.add(saleDetail)
            }
          }

          //todo 5.将自己（orderInfo）写入缓存
          //JSON.toJSONString(OrderInfo)
          val orderInfoJson: String = Serialization.write(orderInfo)
          jedis.set(infoRedisKey, orderInfoJson)

          //设置过期时间
          jedis.expire(infoRedisKey, 30)

        } else {
          //todo orderInfo不存在orderDetail存在
          if (detailOpt.isDefined) {
            val orderDetail: OrderDetail = detailOpt.get
            //todo 6.去对方的（orderInfo）缓存中查找是否有对应的info数据
            if (jedis.exists(infoRedisKey)) {
              val orderInfoJsonStr: String = jedis.get(infoRedisKey)
              //将查出来的orderInfoJson格式的数据转为样例类
              val orderInfo: OrderInfo = JSON.parseObject(orderInfoJsonStr, classOf[OrderInfo])
              val saleDetail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
              details.add(saleDetail)
            } else {
              // todo 7.orderInfo的key不存在，证明数据关联不上，orderDetail来早了
              //将样例类转为json字符串并存入redis
              val orderDetailJson: String = Serialization.write(orderDetail)
              jedis.sadd(detailRedisKey, orderDetailJson)
              //设置过期时间
              jedis.expire(detailRedisKey, 30)
            }
          }
        }
      }
      jedis.close()
      //将集合转为迭代器返回
      details.asScala.toIterator
    })
    noUserSaleDetailDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
