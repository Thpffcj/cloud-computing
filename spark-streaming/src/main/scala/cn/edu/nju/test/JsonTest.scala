package cn.edu.nju.test

import cn.edu.nju.domain.UserData
import com.google.gson.Gson
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSONObject

/**
 * Created by thpffcj on 2019/10/21.
 */
object JsonTest {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("HDFSProcess")

    val sc = new SparkContext(sparkConf)

    val lines = sc.textFile("file:///Users/thpffcj/Public/local-repository/Python-Learning/cloud-computing/utils/test.json")

    val result = lines.map(log => {
      val json = handleMessage2CaseClass(log)

      (json.gameName, 1)
    }).reduceByKey(_ + _)




  }

  def handleMessage2CaseClass(jsonStr: String): UserData = {
    val gson = new Gson()
    gson.fromJson(jsonStr, classOf[UserData])
  }

}
