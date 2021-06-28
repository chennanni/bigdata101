package max.learn.spark.streaming.app

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import max.learn.spark.streaming.utils.{ParamsConf, RedisPool}

/**
  *
  *
  * 功能：使用 Spark Streaming 读取来自 Kafka 的数据；统计每天付费成功的，总订单数 & 总订单金额；最后，存储到 Redis 中
  *
  * ------
  * 流程：数据源 -> Kafka -> Spark Streaming -> Redis
  * ------
  *
  * 前置条件：在 Server 上起好 Zookeeper，Kafka，Redis
  *
  */
object StreamingAnalysisApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamingApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // get kafka stream
    val stream = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](ParamsConf.topic, ParamsConf.kafkaParams)
    )

    // print out stream (to check a bit)
    stream.map(x => x.value()).print()

    // deal with stream
    stream.foreachRDD(rdd => {

      // get data - [flag, fee, time]
      val data = rdd.map(x => JSON.parseObject(x.value()))
      // cache data
      data.cache()

      /**
        * wc
        * rdd.flatMap(_.split(",)).map((_,1)).reduceByKey(_+_)
        */

      // 每天付费成功的总订单数：day flag=1
      data.map(x => {
        val time = x.getString("time")
        val day = time.substring(0,8)
        val flag = x.getString("flag")
        val flagResult = if(flag == "1") 1 else 0
        (day, flagResult)
      }).reduceByKey(_+_).foreachPartition(partition => {
        // save to redis
        val jedis = RedisPool.getJedis()
        partition.foreach(x => {
          jedis.incrBy("ImoocCount-"+x._1, x._2)
        })
      })

      // 每天付费成功的总订单金额
      data.map(x => {
        val time = x.getString("time")
        val day = time.substring(0,8)
        val flag = x.getString("flag")
        val fee = if(flag == "1") x.getString("fee").toLong else 0
        (day, fee)
      }).reduceByKey(_+_).foreachPartition(partition => {
        val jedis = RedisPool.getJedis()
        partition.foreach(x => {
          // save to redis
          jedis.incrBy("ImoocFee-"+x._1, x._2)
        })
      })

      data.unpersist(true)
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
