package mall.streamer.utils

import java.util.Properties
import redis.clients.jedis.Jedis

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/09/08/14:57
 * @Description:
 */
object RedisUtil {

  val config: Properties = PropertiesUtil.load("config.properties")
  val host: String = config.getProperty("redis.host")
  val port: String = config.getProperty("redis.port")

  def getJedisClient(): Jedis = {

    new Jedis(host, port.toInt)
  }
}