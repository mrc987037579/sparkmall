import java.util.{Properties, UUID}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.sun.rowset.internal.Row
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable

object CategoryTop10App {

  def main(args: Array[String]): Unit = {

    //1.创建Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setAppName("CategoryTop10App ").setMaster("local[*]")

    //2.创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //3.获取TaskID(可以不获取，不影响业务)
    val taskId: String = UUID.randomUUID().toString

    //4.加载配置文件内容
    var properties: Properties = PropertiesUtil.load("conditions.properties")

    //5.获取过滤条件
    val conditionJson: String = properties.getProperty("condition.params.json")

    //6.将条件Json转换为对象
    val conditionJsonObj: JSONObject = JSON.parseObject(conditionJson)

    //7.获取JDBC配置文件
    properties = PropertiesUtil.load("config.properties")

    //8.获取Hive数据库名(可以不获取，看Hive在哪里建表)
    val database: String = properties.getProperty("hive.database")

    //9.过滤出来符合条件的数据
    val userVisitActionRDD: RDD[UserVisitAction] = readUserVisitActionToRDD(spark, conditionJsonObj, database)

    //10.注册累加器
    /*
     /*   val categoryCountAccumulator = new CategoryCountAccumulator
        spark.sparkContext.register(categoryCountAccumulator, "categoryAccu")

        //11.通过累计器计算点击、下单以及支付的次数
        //这一步除去了无用数据，并通过map类型的累加器改变值类型为（id，（））
        userVisitActionRDD.foreach(userVisitAction => {
          if (userVisitAction.click_category_id != -1) {
            //将获取到的id与click，order，pay进行拼接
            categoryCountAccumulator.add(userVisitAction.click_category_id + "_click")

          } else if (userVisitAction.order_category_ids != null) {

            userVisitAction.order_category_ids.split(",").foreach { cid =>
              categoryCountAccumulator.add(cid + "_order")
            }
          } else if (userVisitAction.pay_category_ids != null) {
            userVisitAction.pay_category_ids.split(",").foreach(cid => {
              categoryCountAccumulator.add(cid + "_pay")
            })
          }
        })

        //12.获取累加器中的值
        val categoryCount: mutable.HashMap[String, Long] = categoryCountAccumulator.value

        categoryCount.foreach(println)

        //13.将类目名称分组
        val categoryCountInfos: Map[String, mutable.HashMap[String, Long]] = categoryCount.groupBy(_._1.split("_")(0))

        //14. 将类目按照既定条件进行排序
        val sortedCategoryCountInfos: List[(String, mutable.HashMap[String, Long])] = categoryCountInfos.toList.sortWith((c1, c2) => {

          val c1CountInfo: mutable.HashMap[String, Long] = c1._2
          val c2CountInfo: mutable.HashMap[String, Long] = c2._2

          if (c1CountInfo.getOrElse(c1._1 + "_click", 0L) > c2CountInfo.getOrElse(c2._1 + "_click", 0L)) {
            true
          } else if (c1CountInfo.getOrElse(c1._1 + "_click", 0L) == c2CountInfo.getOrElse(c2._1 + "_click", 0L)) {
            if (c1CountInfo.getOrElse(c1._1 + "_order", 0L) > c2CountInfo.getOrElse(c2._1 + "_order", 0L)) {
              true
            } else if (c1CountInfo.getOrElse(c1._1 + "_pay", 0L) > c2CountInfo.getOrElse(c2._1 + "_pay", 0L)) {
              true
            } else {
              false
            }
          } else {
            false
          }
        })

        val result: List[(String, mutable.HashMap[String, Long])] = sortedCategoryCountInfos.take(10)
        //15.将前10名的结果保存到数组中
        val categoryCountInfosArray: List[Array[Any]] = result.map { case (cid, categoryToCount) =>
          Array(taskId, cid, categoryToCount.getOrElse(cid + "_click", 0), categoryCount.getOrElse(cid + "_order", 0), categoryCount.getOrElse(cid + "_pay", 0))
        }*/

        //16.将数据保存到MySQL
        JdbcUtil.executeBatchUpdate("insert into category_top10 values (?,?,?,?,?)", categoryCountInfosArray)
    */

    println("--------------------------需求二-----------------------------------------")
    //对于排名前 10 的品类，分别获取其点击次数排名前 10 的 SessionId
    //（1）按照需求一的结果进行过滤原始数据集，过滤出Session中包含前10品类ID的数据；
    /* {
       val filterlist = result.map(_._1.toString)
       val clickRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(
         userVisitActionRDD => filterlist.contains(userVisitActionRDD.click_category_id.toString)

       )
       //（2）将过滤后的数据按照品类及SessionID进行聚合,计算点击次数；
       val SessionAndProductCount = clickRDD.map(
         clickRDD => ((clickRDD.click_category_id, clickRDD.session_id), 1L)
       ).reduceByKey(_ + _)

       //（3）将维度转换为以品类ID为Key的数据集；
       val productToSessionAndCount = SessionAndProductCount.map {
         case ((click_category_id, session_id), count) => {
           (click_category_id, (click_category_id.toString, session_id, count))
         }
       }
       //（4）按照新的Key进行分组；
       //（5）组内排序取前十；
       val sortedCategorySessionCountRDD: RDD[(String, String, Long)] = productToSessionAndCount.groupByKey().flatMap {
         //由于items包含product_id,所以直接返回items的list
         case (product_id, items) =>
           val top10CategoryTop10Session: List[(String, String, Long)] = items.toList.sortWith {
             case (items1, items2) => items1._3 > items2._3
           }.take(10)
           top10CategoryTop10Session
       }
       val sortedCategorySessionCountArrayRDD = sortedCategorySessionCountRDD.map {
         case (category_id, session_id, count) => Array(taskId, category_id, session_id, count)
       }
       val array: Array[Array[Any]] = sortedCategorySessionCountArrayRDD.collect()
       JdbcUtil.executeBatchUpdate("Insert into category_session_top10 values(?,?,?,?)", array)
       //（6）封装为数组存入MySQL。
       //17.关闭连接
     }*/
    println("--------------------------需求三-----------------------------------------")
    //（1,2,3,4,5,6,7）
  /*  {
      val targetPageFlowArray: Array[String] = conditionJsonObj.getString("targetPageFlow").split(",")
      //（2）按照目标跳转顺序计算所需单页id及跳转id；
      println(targetPageFlowArray.toList)
      val singlePageArray: Array[String] = targetPageFlowArray.drop(1)
      val pageArray: Array[String] = targetPageFlowArray.dropRight(1)
      val targetJumpPage: Array[(String, String)] = pageArray.zip(singlePageArray)
      targetJumpPage.map {
        case (x, y) => s"$x-$y"
      }
      //单页id：1,2,3,4,5,6,
      //跳转id：1-2,2-3,3-4,4-5,5-6,6-7
      //（3）统计出单页访问次数并根据目标页面进行过滤，分子；
      val filterPageRdd: RDD[UserVisitAction] = userVisitActionRDD.filter(
        userVisitActionRDD => singlePageArray.contains(userVisitActionRDD.page_id.toString)
      )
      val singlePageCount: collection.Map[String, Long] = filterPageRdd.map {
        userVisitActionRDD => (userVisitActionRDD.page_id.toString, 1)
      }.countByKey()
      // (1,100),(2,125),(3,150)… …
      //（4）统计出同一个Session内跳转的页面并根据目标跳转顺序进行过滤，分母；
      val sessionToUserActionRDD: RDD[(String, Iterable[(String, Long)])] = userVisitActionRDD.map {
        userVisitActionRDD =>
          (userVisitActionRDD.session_id, (userVisitActionRDD.action_time,
            userVisitActionRDD.page_id
          ))
      }.groupByKey()
      //排序
      val jumpPageAndOne: RDD[(String, Long)] = sessionToUserActionRDD.flatMap {
        case (session, items) =>
          val pageIds: List[String] = items.toList.sortBy(_._1).map(_._2.toString)
          val fromPageIds: List[String] = pageIds.dropRight(1)
          val toPageIds: List[String] = pageIds.drop(1)

          //计算单跳
          val jumPage: List[String] = fromPageIds.zip(toPageIds).map { case (fromPage, toPage) =>
            s"$fromPage-$toPage"
          }
          println(pageIds)
          println(jumPage)
          val filterJumpPageList: List[String] = jumPage.filter(targetJumpPage.contains)
          filterJumpPageList.map((_, 1L))
      }
      val jumpPageCount: collection.Map[String, Long] = jumpPageAndOne.countByKey()
      //(1-2,50),(2-3,80),(3-4,59)… …
      //（5）计算跳转率；
      val jumpPageRatio: Iterable[Array[Any]] = jumpPageCount.map {
        case (jumpPage, count) => Array(taskId, jumpPage, count
          / singlePageCount.getOrElse(jumpPage.split("-")(0), 1L))
      }
      //（6）写入MySQL。
      JdbcUtil.executeBatchUpdate("insert into jump_page_ratio values(?,?,?)", jumpPageRatio)
      //(1-2)的count除以1的count
    }*/
    println("------------------------------------需求四")
    spark.udf.register("cityRatio", new CityTopUDAF)
    // 1）读取用户行为及城市信息表，取出其中的大区信息以及点击品类信息；
    spark.sql("select ci.area,ci.city_name,uv.click_product_id from " +
      "user_visit_action uv join city_info ci on uv.city_id=ci.city_id where " +
      "uv.click_product_id>0").createTempView("tmp_area_product")
    //  2）按照大区以及品类聚合统计点击总数；
    spark.sql("select area,click_product_id,count(*) product_count,cityRatio(city_name) city_click_ratio " +
      "from tmp_area_product group by area,click_product_id")
      .createTempView("tmp_area_product_count")
    // 3）大区内计算品类点击次数的排名；
    spark.sql("select area,click_product_id,product_count,city_click_ratio,rank() " +
      "over(partition by area order by product_count desc) rk " +
      "from tmp_area_product_count").
      createOrReplaceTempView("tmp_sorted_area_product")
    //7.获取TaskId
    //8.取出大区品类前三名,并关联商品表取出商品名称
    val cityCountRatioDF: DataFrame = spark.sql(s"select '$taskId' task_id,area,product_name," +
      s"product_count,city_click_ratio from tmp_sorted_area_product tmp " +
      s"join product_info pi on tmp.click_product_id=pi.product_id where rk<=3")
    //4）使用自定义UDAF函数统计城市占比
    cityCountRatioDF.write.format("jdbc") .option("url", properties.getProperty("jdbc.url"))
      .option("user", properties.getProperty("jdbc.user"))
      .option("password", properties.getProperty("jdbc.password"))
      .option("dbtable", "area_count_info")
      .mode(SaveMode.Append).save()
    spark.close()
  }


  /** *
    * 获取所有符合条件的访问日志
    */
  def readUserVisitActionToRDD(spark: SparkSession, conditionParam: JSONObject, database: String): RDD[UserVisitAction] = {

    import spark.implicits._

    val startDate: AnyRef = conditionParam.get("startDate")

    val endDate: AnyRef = conditionParam.get("endDate")

    val sql: StringBuilder = new StringBuilder("select v.*")
      .append(" from user_visit_action v join user_info i on  v.user_id=i.user_id ")
      .append("where date>='" + startDate + "' and date<='" + endDate + "'")

    val conditionSql = new StringBuilder("")

    if (!conditionParam.getString("startAge").isEmpty) {
      conditionSql.append(" and age>=" + conditionParam.getString("startAge"))
    }
    if (!conditionParam.getString("endAge").isEmpty) {
      conditionSql.append(" and age<=" + conditionParam.getString("endAge"))
    }
    if (!conditionParam.getString("professionals").isEmpty) {
      conditionSql.append(" and professional=" + conditionParam.getString("professional"))
    }
    if (!conditionParam.getString("city").isEmpty) {
      conditionSql.append(" and city=" + conditionParam.getString("city"))
    }
    if (!conditionParam.getString("gender").isEmpty) {
      conditionSql.append(" and gender>=" + conditionParam.getString("gender"))
    }

    sql.append(conditionSql)

    println(sql)

    val userVisitActionDF: DataFrame = spark.sql(sql.toString())

    userVisitActionDF.as[UserVisitAction].rdd
  }

}

class CityTopUDAF extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(StructField("city_name", StringType) :: Nil)

  override def bufferSchema: StructType = StructType(StructField("city_count", MapType(StringType, LongType)) ::
    StructField("total_count", LongType) :: Nil)

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String, Long]()
    buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: sql.Row): Unit = {
    val map = buffer.getAs[Map[String, Long]](0)
    val cityName: String = input.getString(0)
    buffer(0) = map + (cityName -> (map.getOrElse(cityName, 0L) + 1L)) //对city累加
    buffer(1) = buffer.getLong(1) + 1L //总count始终加1
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: sql.Row): Unit = {
    val map1: Map[String, Long] = buffer1.getAs[Map[String, Long]](0)
    val map2: Map[String, Long] = buffer2.getAs[Map[String, Long]](0)
    buffer1(0) = map1.foldLeft(map2) {
      case (m, (k, v)) => m + (k -> (m.getOrElse(k, 0L) + v))
    }
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: sql.Row): Any = {
    val cityCount: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    val totalCount: Long = buffer.getLong(1)
    val top2CityCount: List[(String, Long)] = cityCount.toList.sortWith {
      case (c1, c2) => c1._2 > c2._2
    }.take(2)
    var ratios: List[CityRatio] = top2CityCount.map {
      case (city, count) =>
        val cityTatio: Double = Math.round(count.toDouble * 1000 / totalCount) / 10.toDouble
        CityRatio(city, cityTatio)
    }
    var otherRatio = 100.0
    for (elem <- top2CityCount) {
      otherRatio -= elem._2
    }
     otherRatio= Math.round((otherRatio)*10)/10.0
    val otherCityRatio = CityRatio("其他",otherRatio)
     ratios = ratios:+otherCityRatio
    ratios.mkString(",")
  }
}

case class CityRatio(cityName: String, ratio: Double) {
  override def toString: String = s"$cityName:$ratio%"
}