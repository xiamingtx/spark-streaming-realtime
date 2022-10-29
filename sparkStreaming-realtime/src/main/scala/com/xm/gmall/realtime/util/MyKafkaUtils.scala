package com.xm.gmall.realtime.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util
import scala.collection.mutable
import scala.language.postfixOps

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
/*
  Kafka工具类 用于生产和消费数据
 */
object MyKafkaUtils {
  /*
  消费者配置
  ConsumerConfig
   */
  private val consumerConfigs: mutable.Map[String, Object] = mutable.Map[String, Object](
    // kafka集群位置
//    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "myaliyun:9092",
//    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropsUtils("kafka.bootstrap-servers"),
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS),

    // kv反序列化器
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    // groupId
    // offset提交 自动 手动 默认是true
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    // 自动提交的时间间隔 默认5s
//    ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG
    // offset重置 "latest" "earliest"
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"
    // ......
  )

  /*
    消费者
    基于SparkStreaming消费, 获取到KafkaDStream, 使用默认的offset
   */
  def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String): InputDStream[ConsumerRecord[String, String]] = {
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId) // 动态设置groupId
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs)
    )
    kafkaDStream
  }

  /*
    消费者
    基于SparkStreaming消费, 获取到KafkaDStream, 使用指定的offset
   */
  def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String, offsets: Map[TopicPartition, Long]): InputDStream[ConsumerRecord[String, String]] = {
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId) // 动态设置groupId
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs, offsets) // 指定offsets
    )
    kafkaDStream
  }

  /*
    生产者对象
   */
  val producer: KafkaProducer[String, String] = createProducer()
  /*
    创建生产者对象
   */
  def createProducer(): KafkaProducer[String, String] = {
    val producerConfigs: util.HashMap[String, AnyRef] = new util.HashMap[String, AnyRef]
    // ProducerConfig 生产者配置类
    // kafka集群位置
//    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "myaliyun:9092")
//    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, MyPropsUtils("kafka.bootstrap-servers"))
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS))
    // 序列化器
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    // acks
    producerConfigs.put(ProducerConfig.ACKS_CONFIG, "all")
    // batch.size 16kb
    // linger.ms 0
    // retries Int最大值
    // 幂等性 默认false
    producerConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](producerConfigs)
    producer
  }

  /*
  生产 (按照默认的粘性分区策略)
   */
  def send(topic: String, msg: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, msg))
  }

  /*
  生产 (按照key进行分区)
 */
  def send(topic: String, key: String, msg: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, key, msg))
  }

  /*
  关闭生产者对象
   */
  def close(): Unit = {
    if (producer != null) {
      producer.close()
    }
  }
}
