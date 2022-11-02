package com.xm.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.xm.gmall.realtime.util.{MyKafkaUtils, MyOffsetUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.util
import scala.language.postfixOps

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
/*
  业务数据消费分流
  1. 准备实时处理环境 StreamingContext
  2. 从redis中读取偏移量
  3. 从Kafka中消费数据
  4. 提取偏移量结束点
  5. 处理数据
    5.1 转换数据结构
    5.2 分流
      事实数据 => kafka
      维度数据 => redis
  6. flush kafka缓冲区
  7. 提交offset
 */
object OdsBaseDbApp {
  def main(args: Array[String]): Unit = {
    // 1. 准备实时环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("ods_base_db_app")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val topicName: String = "ODS_BASE_DB_1018"
    val groupId: String = "ODS_BASE_DB_GROUP_1018"
    // 2. 从redis中读取偏移量
    val offsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(topicName, groupId)

    // 3. 从kafka中消费数据
    var KafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty) {
      KafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId, offsets)
    } else {
      KafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)
    }

    // 4. 提取偏移量结束点
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = KafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    // 5. 处理数据
    // 5.1 转换结构
    val jsonObjDStream: DStream[JSONObject] = offsetRangesDStream.map(
      consumerRecord => {
        val dataJson: String = consumerRecord.value
        val jsonObj: JSONObject = JSON.parseObject(dataJson)
        jsonObj
      }
    )
//    jsonObjDStream.print(100)

    // 5.2 分流

    // 事实表清单
    // val factTables: Array[String] = Array[String]("order_info", "order_detail" /* 缺啥补啥 */)
    // 维度表清单
    // val dimTables: Array[String] = Array[String]("user_info", "base_province" /* 缺啥补啥 */)

    // foreach外面 driver 连接对象不能序列化 不能传输
    // foreach里面 foreachPartition外面  driver 连接对象不能序列化 不能传输
    // foreachPartition里面, 循环外面 executor 每分区开启一个连接 用完关闭
    // foreachPartition里面, 循环里面 executor 每条数据开启一个连接 用完关闭 太频繁
    jsonObjDStream.foreachRDD(
      rdd => {
        // 如何动态配置表清单
        // 将表清单维护到redis中 实时任务中动态的到redis中获取表清单
        // 类型: set
        // key: FACT:TABLES  DIM:TABLES
        // value: 表名的集合
        // 写入API: sadd
        // 读取API: smembers
        // 过期: 不过期

        val redisFactKeys: String = "FACT:TABLES"
        val redisDimKeys: String = "DIM:TABLES"
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        // 事实表清单
        // 做成广播变量
        val factTables: util.Set[String] = jedis.smembers(redisFactKeys)
        val factTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(factTables)
        // 维度表清单
        // 做成广播变量
        val dimTables: util.Set[String] = jedis.smembers(redisDimKeys)
        val dimTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dimTables)
        jedis.close()

        rdd.foreachPartition(
          jsonObjIter => {
            // 开启redis连接
            val jedis: Jedis = MyRedisUtils.getJedisFromPool()
            for (jsonObj <- jsonObjIter) {
              // 提取操作类型
              val operType: String = jsonObj.getString("type")
              val opValue: String = operType match {
                case "bootstrap-insert" => "I"
                case "insert" => "I"
                case "update" => "U"
                case "delete" => "D"
                case _ => null
              }

              // 判断操作类型: 1. 明确什么操作 2. 过滤不感兴趣的操作
              if (opValue != null) {
                // 提取表名
                val tableName: String = jsonObj.getString("table")
                if (factTablesBC.value.contains(tableName)) {
                  // 事实数据
                  // 提取数据
                  val data: String = jsonObj.getString("data")
                  // DWD_ORDER_INFO_I DWD_ORDER_INFO_U DWD_ORDER_INFO_D
                  val dwdTopicName: String = s"DWD_${tableName.toUpperCase}_${opValue}_1018"
                  MyKafkaUtils.send(dwdTopicName, data)

                  // 模拟数据延迟
                  if (tableName.equals("order_detail")) {
                    Thread.sleep(200)
                  }
                }
                if (dimTablesBC.value.contains(tableName)) {
                  // 维度数据
                  // 类型: string list set zset hash
                  //      hash: 整个表存成一个hash 要考虑目前数据量的大小和将来数据量增长问题 及 高频访问问题
                  //      hash: 一条数据存成一个hash
                  //      String: 一条数据存成一个jsonString
                  // key: DIM:表名:ID
                  // value: 整条数据的jsonString
                  // 写入API: set
                  // 读取API: get
                  // 过期: 不过期

                  // 提取数据中的id
                  val dataObj: JSONObject = jsonObj.getJSONObject("data")
                  val id: String = dataObj.getString("id")
                  val redisKey: String = s"DIM:${tableName.toUpperCase()}:$id"
                  // 写入redis
                  // 在此处开关redis的操作太频繁
                  // val jedis: Jedis = MyRedisUtils.getJedisFromPool()
                  jedis.set(redisKey, dataObj.toJSONString)
                  // jedis.close()
                }
              }
            }
            jedis.close()
            // 刷新kafka缓冲区
            MyKafkaUtils.flush()
          }
        )
        // 提交offset
        MyOffsetUtils.saveOffset(topicName, groupId, offsetRanges)
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
}
