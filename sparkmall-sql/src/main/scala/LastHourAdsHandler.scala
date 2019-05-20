import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream
import org.json4s.native.JsonMethods
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions

object LastHourAdsHandler {
  private val redisKey = "last_hour_ads_click"

  def statLastHourAds(filteredDStream: DStream[Ads_log]): Unit = {
    val dataFormatter = new SimpleDateFormat("HH-mm")
    val DStreamWithWindow: DStream[Ads_log] = filteredDStream.window(Minutes(2), Minutes(1))
    val hourMinutesCount = DStreamWithWindow.map {
      adsinfo => ((adsinfo.adid, dataFormatter.format(new Date(adsinfo.timestamp))), 1)
    }.reduceByKey(_ + _).map {
      case ((adid, hourMinutes), count) => (adid, (hourMinutes, count))
    }.groupByKey()
    val adsIdHourMintesJson = hourMinutesCount.mapValues {
      item => {
        import org.json4s.JsonDSL._
        JsonMethods.compact(JsonMethods.render(item))
      }
    }
    adsIdHourMintesJson.foreachRDD {
      rdd => {
        rdd.foreachPartition {
          items => {
            import scala.collection.JavaConversions._
            val jedis: Jedis = RedisUtil.getJedisClient
            if (jedis.exists(redisKey)) {
              jedis.del(redisKey)
            }
            jedis.hmset(redisKey, items.toMap)
            jedis.close()
          }
        }
      }
    }

  }
}