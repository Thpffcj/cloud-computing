package cn.edu.nju.test

import java.text.SimpleDateFormat
import java.util.Date

/**
 * Created by thpffcj on 2019/10/24.
 */
object test {

  def main(args: Array[String]): Unit = {

    val time:Long= 1398902400 //秒
    val newtime :String = new SimpleDateFormat("yyyy-MM-dd").format(time*1000)
    println(newtime)

    val time1 = "2017-01-01 08:00:00"
    // 1483228800
    val newtime1 :Long= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time1).getTime / 1000
    println(newtime1)


  }

}
