package max.learn.spark.streaming.utils

import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.StringDeserializer

/**
  * 项目参数配置读取类：读取 application.conf 参数
  */
object ParamsConf {

  private lazy val config = ConfigFactory.load()

  // kafka
  val topic = config.getString("kafka.topic").split(",")
  val groupId = config.getString("kafka.group.id")
  val brokers = config.getString("kafka.broker.list")
  var offset = config.getString("kafka.offset")
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> brokers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> groupId,
    "auto.offset.reset" -> offset,
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  // redis
  val redisHost = config.getString("redis.host")
  val redisDB = config.getInt("redis.db")

  // read from application.conf and print
  def main(args: Array[String]): Unit = {
    println("kafka topic: " + ParamsConf.topic(0))
    println("kafka group id: " + ParamsConf.groupId)
    println("kafka brokers: " + ParamsConf.brokers)
    println("redis host: " + ParamsConf.redisHost)
    println("redis db: " + ParamsConf.redisDB)
  }

}
