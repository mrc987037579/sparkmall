
import java.util.Date

import scala.util.Random

object RandomDate {
  def apply(startDate: Date, endDate: Date, step: Int): RandomDate = {
    val randomDate = new RandomDate()
    val avgStepTime: Long = (endDate.getTime - startDate.getTime) / step
    randomDate.maxTimeStep = avgStepTime * 2
    randomDate.lastDateTime = startDate.getTime
    randomDate
  }

  class RandomDate {
    var lastDateTime = 0L
    var maxTimeStep = 0L

    def getRandomDate: Date = {
      val timeStep: Int = new Random().nextInt(maxTimeStep.toInt)
      lastDateTime = lastDateTime + timeStep

      new Date(lastDateTime)
    }
  }
}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object RandomNum {
  def apply(fromNum: Int, toNum: Int): Int = {
    fromNum + new Random().nextInt(toNum - fromNum + 1)
  }

  def multi(fromNum: Int, toNum: Int, amount: Int, delimiter: String, canRepeat: Boolean): String = {

    var str = ""
    if (canRepeat) {
      val buffer = new ListBuffer[Int]
      while (buffer.size < amount) {
        val randoNum: Int = fromNum + new Random().nextInt(toNum - fromNum)
        buffer += randoNum
        str = buffer.mkString(delimiter)
      }

    } else {
      val set = new mutable.HashSet[Int]()
      while (set.size < amount) {
        val randoNum: Int = fromNum + new Random().nextInt(toNum - fromNum)
        set += randoNum
        str = set.mkString(delimiter)
      }
    }
    str
  }

  def main(args: Array[String]): Unit = {
    println(RandomNum.multi(1, 10, 5, ",", canRepeat = false))
  }

}
import scala.collection.mutable.ListBuffer
import scala.util.Random


case class RanOpt[T](value: T, weight: Int)

object RandomOptions {
  def apply[T](opts: RanOpt[T]*): RandomOptions[T] = {
    val randomOptions = new RandomOptions[T]()
    for (opt <- opts) {
      randomOptions.totalWeight += opt.weight
      for (i <- 1 to opt.weight) {
        randomOptions.optsBuffer += opt.value
      }
    }
    randomOptions
  }

  def main(args: Array[String]): Unit = {
    val productExRandomOpt: RandomOptions[String] = RandomOptions(RanOpt("自营", 70), RanOpt("第三方", 30))
    println(productExRandomOpt.getRandomOpt)
  }

}

class RandomOptions[T](opts: RanOpt[T]*) {

  var totalWeight = 0
  var optsBuffer = new ListBuffer[T]

  def getRandomOpt: T = {
    val randomNum: Int = new Random().nextInt(totalWeight)
    optsBuffer(randomNum)
  }
}
