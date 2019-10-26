package cn.edu.nju.test

import cn.edu.nju.domain.GameDetail
import com.google.gson.Gson

/**
 * Created by thpffcj on 2019/10/25.
 */
object TransformTest {

  def main(args: Array[String]): Unit = {

    jsonToGameDetail("")
  }

  def jsonToGameDetail(jsonStr: String): GameDetail = {
    try {
      val gson = new Gson()
      gson.fromJson(jsonStr, classOf[GameDetail])
    } catch {
      case e: Exception => {
        e.printStackTrace()
        null
      }
    }
  }
}
