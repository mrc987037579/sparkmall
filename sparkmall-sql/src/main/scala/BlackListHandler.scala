import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object BlackListHandler {
  val countKey = "day:userid:adsid"

  var blackList = "blacklist"


  def checkUserToBlackList(filteredDStream: DStream[Ads_log]) = {
    filteredDStream.foreachRDD {
      Rdd => {
        Rdd.foreachPartition { infoit => {
          val JedisClient: Jedis = RedisUtil.getJedisClient
          infoit.foreach {
            info => {
              val dsf = new SimpleDateFormat("yyyy-MM-dd")
              val datestr: String = dsf.format(new Date(info.timestamp))
              val field = s"$datestr:${info.userid}:${info.adid}"
              JedisClient.hincrBy(countKey, field, 1L)
              if (JedisClient.hget(countKey, field).toLong >= 100) {
                JedisClient.sadd(blackList, info.userid)
              }
            }
          }
          JedisClient.close()
        }
        }
      }
    }
  }


  def filterBlackList(adsInfoDStream: DStream[Ads_log], sc: SparkContext): DStream[Ads_log] = {
    adsInfoDStream.transform {
      rdd => {
        val jedisClient = RedisUtil.getJedisClient
        val blackUids = jedisClient.smembers("blacklist")
        val blackListBC = sc.broadcast(blackUids)
        rdd.filter {
          info => {
            !blackListBC.value.contains(info.userid)
          }
        }
      }
    }
  }

}
