package com.xm.gmall.realtime.util

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
/*
  配置类
 */
object MyConfig {
  val KAFKA_BOOTSTRAP_SERVERS: String = "kafka.bootstrap-servers"

  val REDIS_HOST: String = "redis.host"
  val REDIS_PORT: String = "redis.port"
  val REDIS_PASSWORD: String = "redis.password"

  val ES_HOST: String = "es.host"
  val ES_PORT: String = "es.port"
}
