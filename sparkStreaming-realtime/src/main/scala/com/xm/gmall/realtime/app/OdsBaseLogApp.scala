package com.xm.gmall.realtime.app

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.xm.gmall.realtime.bean.{PageActionLog, PageDisplayLog, PageLog, StartLog}
import com.xm.gmall.realtime.util.MyKafkaUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
/*
  日志数据的消费分流
  1. 准备实时处理环境 StreamingContext
  2. 从Kafka中消费数据
  3. 处理数据
    3.1 转换数据结构
    3.2 分流
  4. 写出到DWD层
 */
object OdsBaseLogApp {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    // 可以设定成kafka的topic的分区个数 要注意并行度与分区个数的对应关系
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("ods_base_log_app")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 2. 从kafka中消费数据
    val topicName: String = "ODS_BASE_LOG_1018" // 对应生成器配置中的主题名
    val groupId: String = "ODS_BASE_LOG_1018"
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)

//    kafkaDStream.print(100)
    // 3. 处理数据

    // 3.1 转换数据结构
    //    专用结构 Bean
    //    通用结构 Map JsonObject
    val jsonObjDStream: DStream[JSONObject] = kafkaDStream.map(
      consumerRecord => {
        // 获取ConsumerRecord中的value value就是日志数据
        val log: String = consumerRecord.value()
        // 转换成Json对象
        val jsonObj: JSONObject = JSON.parseObject(log)
        // 返回
        jsonObj
      }
    )

//    jsonObjDStream.print(1000)

    // 3.2 分流
    // 日志数据:
    //  页面访问数据
    //    公共字段
    //    页面字段
    //    曝光数据
    //    事件数据
    //    错误数据
    //  启动数据
    //    公共字段
    //    启动数据
    //    错误数据
    val DWD_PAGE_LOG_TOPIC: String = "DWD_PAGE_LOG_TOPIC_1018" // 页面访问
    val DWD_PAGE_DISPLAY_TOPIC: String = "DWD_PAGE_DISPLAY_TOPIC_1018" // 页面曝光
    val DWD_PAGE_ACTION_TOPIC: String = "DWD_PAGE_ACTION_TOPIC_1018" // 页面事件
    val DWD_START_LOG_TOPIC: String = "DWD_START_LOG_TOPIC_1018" // 启动数据
    val DWD_ERROR_LOG_TOPIC: String = "DWD_ERROR_LOG_TOPIC_1018" // 错误数据

    // 分流规则
    //  错误数据: 不做任何的拆分, 只要包含错误字段 直接整条数据发送到对应的topic
    //  页面数据: 拆分成页面访问, 曝光, 事件 分别发送到对应的topic
    //  启动数据: 发送到对应的topic

    jsonObjDStream.foreachRDD(
      rdd => {
        rdd.foreach(
          jsonObj => {
            // 分流过程
            // 分流错误数据
            val errObj: JSONObject = jsonObj.getJSONObject("err")
            if (errObj != null) {
              // 将错误数据发送到 DWD_ERROR_LOG_TOPIC
              MyKafkaUtils.send(DWD_ERROR_LOG_TOPIC, jsonObj.toJSONString)
            } else {
              // 提取公共字段
              val commonObj: JSONObject = jsonObj.getJSONObject("common")
              val ar: String = commonObj.getString("ar")
              val uid: String = commonObj.getString("uid")
              val os: String = commonObj.getString("os")
              val ch: String = commonObj.getString("ch")
              val isNew: String = commonObj.getString("is_new")
              val md: String = commonObj.getString("md")
              val mid: String = commonObj.getString("mid")
              val vc: String = commonObj.getString("vc")
              val ba: String = commonObj.getString("ba")
              // 提取时间戳
              val ts: Long = jsonObj.getLong("ts")

              // 页面数据
              val pageObj: JSONObject = jsonObj.getJSONObject("page")
              if (pageObj != null) {
                // 提取Page字段
                val pageId: String = pageObj.getString("page_id")
                val pageItem: String = pageObj.getString("item")
                val pageItemType: String = pageObj.getString("item_type")
                val duringTime: Long = pageObj.getLong("during_time")
                val lastPageId: String = pageObj.getString("last_page_id")
                val sourceType: String = pageObj.getString("source_type")

                // 封装成PageLog
                val pageLog: PageLog = PageLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, ts)
                // 发送到 DWD_PAGE_LOG_TOPIC
                // 第二个参数 设置不使用getter和setter方法进行反射 直接操作字段
                MyKafkaUtils.send(DWD_PAGE_LOG_TOPIC, JSON.toJSONString(pageLog, new SerializeConfig(true)))

                // 提取曝光数据
                val displaysJsonArray: JSONArray = jsonObj.getJSONArray("displays")
                if (displaysJsonArray != null && displaysJsonArray.size() > 0) {
                  for (i <- 0 until displaysJsonArray.size()) {
                    // 循环拿到每个曝光
                    val displayObj: JSONObject = displaysJsonArray.getJSONObject(i)
                    // 提取曝光字段
                    val displayType: String = displayObj.getString("display_type")
                    val displayItem: String = displayObj.getString("item")
                    val displayItemType: String = displayObj.getString("item_type")
                    val posId: String = displayObj.getString("pos_id")
                    val order: String = displayObj.getString("order")

                    // 封装成PageDisplayLog
                    val pageDisplayLog: PageDisplayLog = new PageDisplayLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, displayType, displayItem, displayItemType, order, posId, ts)
                    // 写到 DWD_PAGE_DISPLAY_TOPIC
                    MyKafkaUtils.send(DWD_PAGE_DISPLAY_TOPIC, JSON.toJSONString(pageDisplayLog, new SerializeConfig(true)))
                  }
                }

                // 提取事件数据(课下完成)
                val actionArrayObj: JSONArray = jsonObj.getJSONArray("actions")
                if(actionArrayObj != null && actionArrayObj.size() > 0 ){
                  for(i <- 0 until actionArrayObj.size()) {
                    val actionObj: JSONObject = actionArrayObj.getJSONObject(i)
                    val actionId: String = actionObj.getString("action_id")
                    val actionItem: String = actionObj.getString("item")
                    val actionItemType: String = actionObj.getString("item_type")
                    val actionTs: Long = actionObj.getLong("ts")
                    //封装 Bean
                    val pageActionLog = PageActionLog(mid, uid, ar, ch, isNew, md, os, vc, pageId, lastPageId, pageItem, pageItemType, duringTime,actionId,actionItem,actionItemType,actionTs, ts)
                    //发送 Kafka
                    MyKafkaUtils.send(DWD_PAGE_ACTION_TOPIC,JSON.toJSONString(pageActionLog,new SerializeConfig(true)))
                  }
                }
              }
              // 启动数据(课下完成)
              val startObj: JSONObject =
                jsonObj.getJSONObject("start")
              if(startObj != null ) {
                val entry: String = startObj.getString("entry")
                val loadingTimeMs: Long = startObj.getLong("loading_time_ms")
                val openAdId: String = startObj.getString("open_ad_id")
                val openAdMs: Long = startObj.getLong("open_ad_ms")
                val openAdSkipMs: Long = startObj.getLong("open_ad_skip_ms")
                //封装 Bean
                val startLog = StartLog(mid, uid, ar, ch, isNew, md, os, vc, entry, openAdId, loadingTimeMs, openAdMs, openAdSkipMs, ts)
                //发送 Kafka
                MyKafkaUtils.send(DWD_START_LOG_TOPIC, JSON.toJSONString(startLog, new SerializeConfig(true)))
              }
            }
          }
        )
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
}
