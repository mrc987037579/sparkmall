import org.apache.spark.streaming.dstream.DStream
import org.json4s.native.JsonMethods
import redis.clients.jedis.Jedis

import scala.collection.immutable.StringOps

object AreaAdsTop3 {


  def statAreaAdsTop3(dayAreaCityAdsCount: DStream[(String, Int)]): Unit = {
    val dayAreaAdsCount: DStream[((String, String, String), Int)] = dayAreaCityAdsCount.map {
      case (dayAreaAdsCity, count) =>
        val strings: Array[String] = dayAreaAdsCity.split(":")
        ((strings(0), strings(1), strings(3)), count)
    }.reduceByKey(_ + _)
    val DayAreatoAdsCount: DStream[((String, String), (String, Int))] = dayAreaAdsCount.map {
      case (((day, area, ads), count)) => ((day, area), (ads, count))
    }
    val DayAreatoAdsCountGroup: DStream[((String, String), Iterable[(String, Int)])] = DayAreatoAdsCount.groupByKey()
    //(日期，地区)，（广告id，总数）
    val DayAreatoAdsCountGrouptop3 = DayAreatoAdsCountGroup.mapValues {
      case (itms) => itms.toList.sortWith {
        case (c1, c2) => c1._2 > c1._2
      }.take(3)
    }
    //
    val dayToAreaAdsCount: DStream[(String, (String, List[(String, Int)]))] = DayAreatoAdsCountGrouptop3.map {
      case (((day, area), list)) =>
        (day, (area, list))
    }
    val dayToAreaAdsCountforJson = dayToAreaAdsCount.groupByKey()
    val dayToAreaAdsCountJson = dayToAreaAdsCountforJson.mapValues {
      import org.json4s.JsonDSL._
      items => {
        val map: Map[String, List[(String, Int)]] = items.toMap
        map.map {
          case (area, items) =>
            val str: String = JsonMethods.compact(JsonMethods.render(items))
            (area, str)
        }
      }
    }
    dayToAreaAdsCountJson.foreachRDD {
      rdd => {
        rdd.foreachPartition {
          items => {
            val jedis: Jedis = RedisUtil.getJedisClient

            items.foreach {
              case (date, map) =>
                import scala.collection.JavaConversions._
                import scala.collection.JavaConversions._
                jedis.hmset("top3_ads_per_day" + date, map)
            }
            jedis.close()
          }
        }

      }
    }


  }
}
