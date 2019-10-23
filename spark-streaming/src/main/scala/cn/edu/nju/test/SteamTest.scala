package cn.edu.nju.test

import cn.edu.nju.domain.{CommentLog, GameDetail, ReviewsChart, SteamLog, UserData}
import com.google.gson.Gson
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by thpffcj on 2019/10/21.
 */
object SteamTest {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("HDFSProcess")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.checkpoint("/Users/thpffcj/Public/file/cloud_checkpoint/hdfs_process")

    val rawData = ssc.socketTextStream("localhost", 9999)
//    val rawData = ssc.textFileStream("hdfs://172.19.170.131:9000/cloud-computing/")

    val data = rawData.filter(rdd => !rdd.isEmpty).map(line=> {
      val log = line.split("\t")
      SteamLog(log(0), log(1), log(2), log(3), log(4), log(5),log(6))
    })

    // 取出用户标签
    val userTags = data.map(steamLog => {

      val gameDetail = jsonToGameDetail(steamLog.game_detail)
      gameDetail.user_tags.toString.replace(" ", "")
    })

    // 标签统计
    val tagsNumber = userTags.flatMap(line => line.substring(1, line.length - 1).split(","))
        .map(tag => (tag, 1)).updateStateByKey[Int](updateFunction _)

    tagsNumber.print()

    val recent = data.map(steamLog => {
      val gameDetail = jsonToGameDetail(steamLog.game_detail)
      val reviewsChart = jsonToReviewsChart(gameDetail.reviewsChart.toString)
      reviewsChart.recent
    })

    recent.print()

    ssc.start()
    ssc.awaitTermination()
  }


  def jsonToGameDetail(jsonStr: String): GameDetail = {
    val gson = new Gson()
    gson.fromJson(jsonStr, classOf[GameDetail])
  }

  def jsonToReviewsChart(jsonStr: String): ReviewsChart = {
    val gson = new Gson()
    gson.fromJson(jsonStr, classOf[ReviewsChart])
  }

  /**
   * 把当前的数据去更新已有的或者是旧的数据
   * @param currentValues 当前数据
   * @param preValues 旧数据
   * @return
   */
  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)
    Some(current + pre)
  }
}
