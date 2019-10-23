package cn.edu.nju.test

import cn.edu.nju.domain.{GameDetail, ReviewsChart, UserData}
import com.google.gson.Gson
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSONObject

/**
 * Created by thpffcj on 2019/10/21.
 */
object JsonTest {

  def main(args: Array[String]): Unit = {

//    val result1 = jsonToGameDetail("{\"reviewsChart\": {\"end_date\": 1571616000, \"rollups\": [{\"recommendations_down\": 34, \"date\": 1571270400, \"recommendations_up\": 167}], \"recent\": [{\"recommendations_down\": 1, \"date\": 1571616000, \"recommendations_up\": 7}], \"rollup_type\": \"week\", \"weeks\": [], \"start_date\": 1569456000}, \"support_tags\": [\"单人\", \"在线合作\", \"Steam 成就\"], \"user_tags\": [\"动漫\", \"砍杀\", \"冒险\", \"好评原声音轨\"], \"user_reviews\": {\"发行日期:\": \"2019年9月26日\", \"开发商:\": \"BANDAI NAMCO Studios\", \"发行商:\": \"BANDAI NAMCO Entertainment\"}}")
//    print(result1)

    val result2 = jsonToReviewsChart("{\"end_date\":1.571616E9,\"weeks\":[],\"rollup_type\":\"week\",\"recent\":[{\"recommendations_down\":1.0,\"date\":1.571616E9,\"recommendations_up\":7.0}],\"rollups\":[{\"recommendations_down\":34.0,\"date\":1.5712704E9,\"recommendations_up\":167.0}],\"start_date\":1.569456E9}")
    print(result2)

  }

  def jsonToGameDetail(jsonStr: String): GameDetail = {
    val gson = new Gson()
    gson.fromJson(jsonStr, classOf[GameDetail])
  }

  def jsonToReviewsChart(jsonStr: String): ReviewsChart = {
    val gson = new Gson()
    gson.fromJson(jsonStr, classOf[ReviewsChart])
  }

}
