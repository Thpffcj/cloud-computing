package cn.edu.nju.test

import java.text.SimpleDateFormat

import cn.edu.nju.utils.DateUtils

/**
 * Created by thpffcj on 2019/10/25.
 */
object DateTest {

  def main(args: Array[String]): Unit = {

    val startDate = "2017-03-01 08:00:00"
    val startTime: Int = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(startDate).getTime / 1000).toInt

    val endDate = "2019-10-01 08:00:00"
    val endTime : Int = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(endDate).getTime / 1000).toInt

    for (date <- DateUtils.getSteamDates()) {
      println((new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date).getTime / 1000).toInt)
    }
  }
}
