package com.xm.gmall.realtime.app

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.xm.gmall.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.xm.gmall.realtime.util.{MyEsUtils, MyKafkaUtils, MyOffsetUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.{Jedis, Pipeline}

import java.time.{LocalDate, Period}
import java.util
import scala.collection.mutable.ListBuffer

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
/*
  订单宽表任务

  1. 准备实时环境
  2. 从Redis中读取offset * 2
  3. 从kafka中消费数据 * 2
  4. 提取offset * 2
  5. 数据处理
    5.1 转换结构
    5.2 维度关联
    5.3 双流join
  6. 写入ES
  7. 提交offset * 2
 */
object DwdOrderApp {
  def main(args: Array[String]): Unit = {
    // 0. 还原状态
    revertState()
    // 1. 准备环境
    val sparkConf: SparkConf = new SparkConf().setAppName("dwd_order_app").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 2. 读取offset
    // order_info
    val orderInfoTopicName: String = "DWD_ORDER_INFO_I_1018"
    val orderInfoGroup: String = "DWD_ORDER_INFO_GROUP"
    val orderInfoOffsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(orderInfoTopicName, orderInfoGroup)

    // order_detail
    val orderDetailTopicName: String = "DWD_ORDER_DETAIL_I_1018"
    val orderDetailGroup: String = "DWD_ORDER_DETAIL_GROUP"
    val orderDetailOffsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(orderDetailTopicName, orderDetailGroup)

    // 3. 从kafka中消费数据
    // order_info
    var orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderInfoOffsets != null && orderInfoOffsets.nonEmpty) {
      orderInfoKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderInfoTopicName, orderInfoGroup, orderInfoOffsets)
    } else {
      orderInfoKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderInfoTopicName, orderInfoGroup)
    }

    // order_detail
    var orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderDetailOffsets != null && orderDetailOffsets.nonEmpty) {
      orderDetailKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderDetailTopicName, orderDetailGroup, orderDetailOffsets)
    } else {
      orderDetailKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderDetailTopicName, orderDetailGroup)
    }

    // 4. 提取offset
    // order_info
    var orderInfoOffsetRanges: Array[OffsetRange] = null
    val orderInfoOffsetDStream: DStream[ConsumerRecord[String, String]] = orderInfoKafkaDStream.transform(
      rdd => {
        orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    // order_detail
    var orderDetailOffsetRanges: Array[OffsetRange] = null
    val orderDetailOffsetDStream: DStream[ConsumerRecord[String, String]] = orderDetailKafkaDStream.transform(
      rdd => {
        orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    // 5. 处理数据
    // 5.1 转换数据结构
    val orderInfoDStream: DStream[OrderInfo] = orderInfoOffsetDStream.map(
      consumerRecord => {
        val value: String = consumerRecord.value()
        val orderInfo: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])
        orderInfo
      }
    )
//    orderInfoDStream.print(100)

    val orderDetailDStream: DStream[OrderDetail] = orderDetailOffsetDStream.map(
      consumerRecord => {
        val value: String = consumerRecord.value()
        val orderDetail: OrderDetail = JSON.parseObject(value, classOf[OrderDetail])
        orderDetail
      }
    )
//    orderDetailDStream.print(100)

    // 5.2 维度关联
    // order_info
    val orderInfoDimDStream: DStream[OrderInfo] = orderInfoDStream.mapPartitions(
      orderInfoIter => {
        // val orderInfos: ListBuffer[OrderInfo] = ListBuffer[OrderInfo]()
        val orderInfos: List[OrderInfo] = orderInfoIter.toList
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        for (orderInfo <- orderInfos) {
          // 关联用户维度
          val uid: Long = orderInfo.user_id
          val redisUserKey: String = s"DIM:USER_INFO:$uid"
          val userInfoJson: String = jedis.get(redisUserKey)
          val userInfoJsonObj: JSONObject = JSON.parseObject(userInfoJson)
          // 提取性别
          val gender: String = userInfoJsonObj.getString("gender")
          // 提取生日
          val birthday: String = userInfoJsonObj.getString("birthday")
          // 换算年龄
          val birthdayLd: LocalDate = LocalDate.parse(birthday)
          val nowLd: LocalDate = LocalDate.now()
          val period: Period = Period.between(birthdayLd, nowLd)
          val age: Int = period.getYears
          // 补充到对象中
          orderInfo.user_gender = gender
          orderInfo.user_age = age
          // 关联地区维度
          val provinceId: Long = orderInfo.province_id
          val redisProvinceKey: String = s"DIM:BASE_PROVINCE:$provinceId"
          val provinceJson: String = jedis.get(redisProvinceKey)
          val provinceJsonObj: JSONObject = JSON.parseObject(provinceJson)

          val provinceName: String = provinceJsonObj.getString("name")
          val provinceAreaCode: String = provinceJsonObj.getString("area_code")
          val province3166: String = provinceJsonObj.getString("iso_3166_2")
          val provinceIsoCode: String = provinceJsonObj.getString("iso_code")
          // 补充到对象中
          orderInfo.province_name = provinceName
          orderInfo.province_area_code = provinceAreaCode
          orderInfo.province_3166_2_code = province3166
          orderInfo.province_iso_code = provinceIsoCode
          // 处理日期字段
          val create_time: String = orderInfo.create_time
          val createDtHr: Array[String] = create_time.split(" ")
          val createDate: String = createDtHr(0)
          val createHr: String = createDtHr(1).split(":")(0)
          // 补充到对象中
          orderInfo.create_date = createDate
          orderInfo.create_hour = createHr

          // orderInfos.append(orderInfo)
        }
        jedis.close()
        orderInfos.iterator
      }
    )

//    orderInfoDimDStream.print(100)

    // 5.3 双流join
    // 内连接 join  结果取交集
    // 外连接
    //  左外连 leftOuterJoin 左表所有+右表的匹配 分析清除主(驱动表)从(匹配表)表
    //  右外连 rightOuterJoin  左表的匹配+右表的所有 分析清除主(驱动表)从(匹配表)表
    //  全外连 fullOuterJoin  两张表的所有

    // 从数据库层面: order_info表中的数据 和 order_detail表中的数据一定能关联成功
    // 从流的层面: order_info 和 order_detail是两个流 流的join只能是同一个批次的数据才能进行join
    // 如果两个表的数据进入到不同批次中 就会join不成功
    // 数据延迟导致的数据没有进入到同一个批次 在实时处理中是正常现象 我们可以接受因为延迟导致最终的结果延迟
    // 我们不能接受 因为延迟导致的数据丢失
    val orderInfoKVDStream: DStream[(Long, OrderInfo)] = orderInfoDimDStream.map(orderInfo => (orderInfo.id, orderInfo))
    val orderDetailKVDStream: DStream[(Long, OrderDetail)] = orderDetailDStream.map(orderDetail => (orderDetail.order_id, orderDetail))

    // val orderJoinDStream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoKVDStream.join(orderDetailKVDStream)
    // orderJoinDStream.print(1000)

    // 解决:
    // 1. 扩大采集周期 治标不治本
    // 2. 使用窗口 治标不治本 还要考虑数据去重 spark的缺点
    // 3. 首先使用fullOuterJoin 保证join成功或没有成功的数据都出现到结果中
    //    让双方都多两步操作 到缓存中找对的人 把自己写到缓存中
    val orderJoinDStream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoKVDStream.fullOuterJoin(orderDetailKVDStream)

    val orderWideDStream: DStream[OrderWide] = orderJoinDStream.mapPartitions {
      orderJoinIter => {
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        val orderWides: ListBuffer[OrderWide] = ListBuffer[OrderWide]()
        for ((key, (orderInfoOp, orderDetailOp)) <- orderJoinIter) {
          // orderInfo有 orderDetail有
          if (orderInfoOp.isDefined) {
            // 取出orderInfo
            val orderInfo: OrderInfo = orderInfoOp.get
            if (orderDetailOp.isDefined) {
              // 取出orderDetail
              val orderDetail: OrderDetail = orderDetailOp.get
              // 组装成orderWide
              val orderWide: OrderWide = new OrderWide(orderInfo, orderDetail)
              // 放入到结果集中
              orderWides.append(orderWide)
            }
            // orderInfo有 orderDetail没有

            // orderInfo写缓存
            // 类型: string
            // key: ORDERJOIN:ORDER_INFO:ID
            // value: json
            // 写入API: set
            // 读取API: get
            // 是否过期: 24h
            val redisOrderInfoKey: String = s"ORDER_JOIN:ORDER_INFO:${orderInfo.id}"
            // jedis.set(redisOrderInfoKey, JSON.toJSONString(orderInfo, new SerializeConfig(true)))
            // jedis.expire(redisOrderInfoKey, 24 * 3600)
            jedis.setex(redisOrderInfoKey, 24 * 3600, JSON.toJSONString(orderInfo, new SerializeConfig(true)))

            // orderInfo读缓存
            val redisOrderDetailKey: String = s"ORDER_JOIN:ORDER_DETAIL:${orderInfo.id}"
            val orderDetails: util.Set[String] = jedis.smembers(redisOrderDetailKey)
            if (orderDetails != null && orderDetails.size() > 0) {
              import scala.collection.JavaConverters._
              for (orderDetailJson <- orderDetails.asScala) {
                val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
                // 组装成orderWide
                val orderWide: OrderWide = new OrderWide(orderInfo, orderDetail)
                // 加入到结果集中
                orderWides.append(orderWide)
              }
            }
          } else {
            // orderInfo没有 orderDetail有
            val orderDetail: OrderDetail = orderDetailOp.get
            // 读缓存
            val redisOrderInfoKey: String = s"ORDER_JOIN:ORDER_INFO:${orderDetail.order_id}"
            val orderInfoJson: String = jedis.get(redisOrderInfoKey)
            if (orderInfoJson != null && orderInfoJson.nonEmpty) {
              val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
              // 组装成orderWide
              val orderWide: OrderWide = new OrderWide(orderInfo, orderDetail)
              // 加入到结果集中
              orderWides.append(orderWide)
            } else {
              // 写缓存
              // 类型: set
              // key: ORDERJOIN:ORDER_DETAIL:ORDER_ID
              // value: json,json,...
              // 写入API: sadd
              // 读取API: smembers
              // 是否过期: 24h
              val redisOrderDetailKey: String = s"ORDER_JOIN:ORDER_DETAIL:${orderDetail.order_id}"
              jedis.sadd(redisOrderDetailKey, JSON.toJSONString(orderDetail, new SerializeConfig(true)))
              jedis.expire(redisOrderDetailKey, 24 * 3600)
            }
          }
        }
        jedis.close()
        orderWides.iterator
      }
    }

    // orderWideDStream.print(1000)

    // 1. 索引分割 通过索引模板控制mapping setting aliases
    // 2. 使用工具类
    //写入 es
    orderWideDStream.foreachRDD(
      rdd => {
        //driver
        rdd.foreachPartition(
          orderWideIter => {
            //executor
            val orderWideList: List[(String, OrderWide)] = orderWideIter.toList.map(orderWide => (orderWide.detail_id.toString , orderWide))
            if(orderWideList.nonEmpty){
              val orderWideT: (String , OrderWide) = orderWideList.head

              val dt: String = orderWideT._2.create_date
              val indexName: String = s"gmall_order_wide_$dt"

              MyEsUtils.bulkSave(indexName, orderWideList)
            }
          }
        )
        //提交偏移量
        MyOffsetUtils.saveOffset(orderInfoTopicName, orderInfoGroup , orderInfoOffsetRanges)
        MyOffsetUtils.saveOffset(orderDetailTopicName, orderDetailGroup,orderDetailOffsetRanges)
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 状态还原
   *
   * 在每次启动实时任务时 进行一次状态还原 以es为准 将所有的mid提出来 覆盖到redis中
   */
  def revertState(): Unit = {
    // 从es中查到所有的mid
    val date: LocalDate = LocalDate.now()
    val indexName: String = s"gmall_dau_info_1018_$date"
    val fieldName: String = "mid"
    val mids: List[String] = MyEsUtils.searchField(indexName, fieldName)
    // 删除redis中记录的状态(所有的mid)
    val jedis: Jedis = MyRedisUtils.getJedisFromPool()
    val redisDauKey: String = s"DAU:$date"
    jedis.del(redisDauKey)

    // 将从es中查询到的mid覆盖到Redis中
    if (mids.nonEmpty) {
      val pipeline: Pipeline = jedis.pipelined()
      for (mid <- mids) {
        pipeline.sadd(redisDauKey, mid) // 不会直接到redis中执行
      }

      pipeline.sync() // 执行
    }
    jedis.close()
  }
}
