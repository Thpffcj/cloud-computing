package cn.edu.nju.utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

import scala.collection.mutable.ListBuffer

/**
 * Created by thpffcj on 2019/10/17.
 */
object DateUtils {

  val YYYYMMDDHHMMSS_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val TARGE_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmss")


  def getTime(time: String) = {
    YYYYMMDDHHMMSS_FORMAT.parse(time).getTime
  }

  def parseToMinute(time :String) = {
    TARGE_FORMAT.format(new Date(getTime(time)))
  }

  def getSteamDates(): ListBuffer[String] = {
    val dates = new ListBuffer[String]
    dates.append("2017-01-01 08:00:00")
    dates.append("2017-02-01 08:00:00")
    dates.append("2017-03-01 08:00:00")
    dates.append("2017-04-01 08:00:00")
    dates.append("2017-05-01 08:00:00")
    dates.append("2017-06-01 08:00:00")
    dates.append("2017-07-01 08:00:00")
    dates.append("2017-08-01 08:00:00")
    dates.append("2017-09-01 08:00:00")
    dates.append("2017-10-01 08:00:00")
    dates.append("2017-11-01 08:00:00")
    dates.append("2017-12-01 08:00:00")
    dates.append("2018-01-01 08:00:00")
    dates.append("2018-02-01 08:00:00")
    dates.append("2018-03-01 08:00:00")
    dates.append("2018-04-01 08:00:00")
    dates.append("2018-05-01 08:00:00")
    dates.append("2018-06-01 08:00:00")
    dates.append("2018-07-01 08:00:00")
    dates.append("2018-08-01 08:00:00")
    dates.append("2018-09-01 08:00:00")
    dates.append("2018-10-01 08:00:00")
    dates.append("2018-11-01 08:00:00")
    dates.append("2018-12-01 08:00:00")
    dates.append("2019-01-01 08:00:00")
    dates.append("2019-02-01 08:00:00")
    dates.append("2019-03-01 08:00:00")
    dates.append("2019-04-01 08:00:00")
    dates.append("2019-05-01 08:00:00")
    dates.append("2019-06-01 08:00:00")
    dates.append("2019-07-01 08:00:00")
    dates.append("2019-08-01 08:00:00")
    dates.append("2019-09-01 08:00:00")
    dates.append("2019-10-01 08:00:00")

    dates
  }

  def main(args: Array[String]): Unit = {

    println(parseToMinute("2017-10-22 14:46:01"))
  }
}