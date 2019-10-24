package cn.edu.nju.test

/**
 * Created by thpffcj on 2019/10/24.
 */
object test {

  def main(args: Array[String]): Unit = {
    for (time <- Range(1398902400, 1401580800 + 1, 2678400)){
      println(time)
    }
  }

}
