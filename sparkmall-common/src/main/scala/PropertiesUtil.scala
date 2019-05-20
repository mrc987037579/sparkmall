
import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtil {
  def load(propertieName:String): Properties ={
    val prop=new Properties();
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }
}
case class UserVisitAction(date: String,
                           user_id: Long,
                           session_id: String,
                           page_id: Long,
                           action_time: String,
                           search_keyword: String,
                           click_category_id: Long,
                           click_product_id: Long,
                           order_category_ids: String,
                           order_product_ids: String,
                           pay_category_ids: String,
                           pay_product_ids: String,
                           city_id:Long
                          )
case class UserInfo(user_id: Long,
                    username: String,
                    name: String,
                    age: Int,
                    professional: String,
                    gender: String
                   )
case class ProductInfo(product_id: Long,
                       product_name: String,
                       extend_info: String
                      )
case class CityInfo (city_id:Long,
                     city_name:String,
                     area:String
                    )