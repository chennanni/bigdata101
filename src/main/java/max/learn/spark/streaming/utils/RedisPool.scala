package max.learn.spark.streaming.utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * Redis连接池
  *
  * 前置条件：需要在 Server(hadoop000) 上 start redis-server
  *
  * 启动方式：./redis-server --protected-mode no
  * （因为没配置密码，所以把保护模式关了）
  *
  */
object RedisPool {

  val poolConfig = new GenericObjectPoolConfig()
  poolConfig.setMaxIdle(10)
  poolConfig.setMaxTotal(1000)

  private lazy val jedisPool = new JedisPool(poolConfig, ParamsConf.redisHost)

  def getJedis() = {
    val jedis = jedisPool.getResource
    jedis.select(ParamsConf.redisDB)
    jedis
  }

  def main(args: Array[String]): Unit = {
    val jedis = RedisPool.getJedis()
    jedis.set("testmsg", "hello world redis")
    println(jedis.get("testmsg"))
  }

}