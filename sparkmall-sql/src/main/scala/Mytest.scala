import java.text.SimpleDateFormat
import java.util.Date

object Mytest {
  def main(args: Array[String]): Unit = {
    val dayString: String = new SimpleDateFormat().format("yyyy-MM-dd")
    val datestr: String = dayString.format(new Date(111145345345111L))
    println(datestr)
  }
}
