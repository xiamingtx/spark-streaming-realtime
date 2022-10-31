package com.xm.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.xm.gmall.realtime.bean.PageLog
import com.xm.gmall.realtime.util.{MyKafkaUtils, MyOffsetUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import scala.collection.mutable.ListBuffer

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
/*
  日活宽表
  1. 准备实时数据
  2. 从Redis中读取偏移量
  3. 从kafka中消费数据
  4. 提取偏移量结束点
  5. 处理数据
      5.1 转换数据结构
      5.2 去重
      5.3 维度关联
  6. 写入ES
  7. 提交offset
 */
object DwdDauApp {
  def main(args: Array[String]): Unit = {
    // 1. 准备实时环境
    val sparkConf: SparkConf = new SparkConf().setAppName("dwd_dau_app").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 2. 从Redis中读取offset
    val topicName: String = "DWD_PAGE_LOG_TOPIC_1018"
    val groupId: String = "DWD_DAU_GROUP"
    val offsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(topicName, groupId)

    // 3. 从kafka中消费数据
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty) {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId, offsets)
    } else {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)
    }

    // 4. 提取offset结束点
    var offsetRanges: HasOffsetRanges = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges]
        rdd
      }
    )

    // 5. 转换结构
    val pageLogDStream: DStream[PageLog] = offsetRangesDStream.map(
      consumerRecord => {
        val value: String = consumerRecord.value()
        val pageLog: PageLog = JSON.parseObject(value, classOf[PageLog])
        pageLog
      }
    )

    pageLogDStream.cache()
    pageLogDStream.foreachRDD(
      rdd => println("自我审查前: " + rdd.count())
    )
    // 5.2 去重
    // 自我审查: 将页面访问数据中last_page_id不为空的数据过滤掉
    val filterDStream: DStream[PageLog] = pageLogDStream.filter(
      pageLog => pageLog.last_page_id == null
    )

    filterDStream.cache()
    filterDStream.foreachRDD(rdd => {
      println("自我审查后: " + rdd.count())
      println("------------------------")
    })

    // 第三方审查: 通过redis将当日活跃的mid维护起来 自我审查后的每条数据需要到redis中进行比对去重
    // redis中如何维护日货状态
    // 类型: list set
    // key: DAU:DATE
    // value: mid的集合
    // 写入API: lpush/rpush sadd
    // 读取API: lrange smembers
    // 过期: 24h过期

    // filterDStream.filter() // 每条数据执行一次 redis的连接太频繁
    // [A, B, C] => [AA, BB]
    filterDStream.mapPartitions(
      pageLogIter => {
        // 存储要的数据
        val pageLogs: ListBuffer[PageLog] = ListBuffer[PageLog]()
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        for (pageLog <- pageLogIter) {
          // 提取每条数据中的mid(我们的日活基于mid 也可以基于uid)
          val mid: String = pageLog.mid
          // 获取日期 因为我们要测试不同天的数据 所以不能直接获取系统时间
          val ts: Long = pageLog.ts
          val date: Date = new Date(ts)
          val dateStr: String = sdf.format(date)
          val redisDauKey: String = s"DAU:$dateStr"
          // redis的判断是否包含操作
          // list
          val mids: util.List[String] = jedis.lrange(redisDauKey, 0, -1)
          if (!mids.contains(mid)) {
            jedis.lpush(redisDauKey, mid)
            pageLogs.append(pageLog)
          }
          // set
          val setMids: util.Set[String] = jedis.smembers(redisDauKey)
          if (!setMids.contains(mid)) {
            jedis.sadd(redisDauKey, mid)
            pageLogs.append(pageLog)
          }
        }
        jedis.close()
        pageLogs.iterator
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
