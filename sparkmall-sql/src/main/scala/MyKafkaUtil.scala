import java.lang
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object MyKafkaUtil {
  private val properties: Properties = PropertiesUtil.load("config.properties")
  private val broker_list: String = properties.getProperty("kafka.broker.list")
  private val kafkaParam = Map(

    "bootstrap.servers" -> broker_list,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],

    //消费者组
    "group.id" -> "commerce-consumer-group",

    //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
    //可以使用这个配置，latest自动重置偏移量为最新的偏移量
    "auto.offset.reset" -> "latest",

    //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
    //如果是false，会需要手动维护kafka偏移量
    "enable.auto.commit" -> (true: lang.Boolean)
  )

  def getKafkaStream(topic: String, ssc: StreamingContext) = {
    val dsStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))
    dsStream
  }

  def main(args: Array[String]) = {
    val conf: SparkConf = new SparkConf().setAppName("MyKafkaUtil").setMaster("local[*]")
    val sparkContext = new SparkContext(conf)
    val streamingContext = new StreamingContext(sparkContext, Seconds(5))
    val DsStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("test", streamingContext)
    val adsInfoDStream: DStream[Ads_log] = DsStream.map {
      DsStream =>
        val strings: Array[String] = DsStream.value().split(",")
        Ads_log(strings(0).toLong, strings(1), strings(2), strings(3), strings(4))
    }
    val filteredDStream: DStream[Ads_log] = BlackListHandler.filterBlackList(adsInfoDStream, sparkContext)
    BlackListHandler.checkUserToBlackList(filteredDStream)
    val DayAreaCityAdscount: DStream[(String, Int)] = DayAreaCityAdsApp.statAreaCityAdsPerDay(filteredDStream,sparkContext)
    println("____________________需求八——————————————————————————")
    DayAreaCityAdscount.cache()
    AreaAdsTop3.statAreaAdsTop3(DayAreaCityAdscount)
    streamingContext.start()
    streamingContext.awaitTermination()
    LastHourAdsHandler.statLastHourAds(filteredDStream)
  }
}
case class Ads_log(timestamp: Long, area: String, city: String, userid: String, adid: String)
object RedisUtil {
  var jedisPool: JedisPool = _

  def getJedisClient= {
    if (jedisPool == null) {
      println("开辟一个连接池")
      val properties: Properties = PropertiesUtil.load("config.properties")
      val host: String = properties.getProperty("redis.host")
      val port: String = properties.getProperty("redis.port")

      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100) //最大连接数
      jedisPoolConfig.setMaxIdle(20) //最大空闲
      jedisPoolConfig.setMinIdle(20) //最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(500) //忙碌时等待时长 毫秒
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试
      jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)

    }
    println(s"jedisPool.getNumActive = ${jedisPool.getNumActive}")
    println("获得一个连接")
   val resource: Jedis = jedisPool.getResource
    resource
  }
}
