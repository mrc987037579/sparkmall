import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DayAreaCityAdsApp {
  def statAreaCityAdsPerDay(adsInfoDStream: DStream[Ads_log], sc: SparkContext) ={
sc.setCheckpointDir("./ck")
    val key="day:area:city:ads"
    val DayAreaCityAdscount: DStream[(String, Int)] = adsInfoDStream.map {
      info => (info.toString, 1)
    }.reduceByKey(_ + _).updateStateByKey(
      (seq: Seq[Int], opt: Option[Int]) => Some(seq.sum + opt.getOrElse(0))
    )
    DayAreaCityAdscount.foreachRDD(
      rdd=>{
        val jedisClient: Jedis = RedisUtil.getJedisClient
        val totalCountArray: Array[(String, Int)] = rdd.collect()
        totalCountArray.foreach{
          case (field,count)=>jedisClient.hset(key,field,count.toString)
        }
        jedisClient.close()
      })
    DayAreaCityAdscount
  }
}
