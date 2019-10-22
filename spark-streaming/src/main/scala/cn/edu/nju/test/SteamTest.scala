package cn.edu.nju.test

import cn.edu.nju.domain.{CommentLog, GameDetail, SteamLog, UserData}
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

    val data = ssc.socketTextStream("localhost", 9999)

    val userTags = data.map(log => {
      val steamLog = jsonToStreamLog(log)

      val gameDetail = jsonToGameDetail(steamLog.game_detail.toString)

      gameDetail.user_tags.toString
    })

    val tagsNumber = userTags.flatMap(line => line.substring(1, line.length - 1).split(","))
        .map(tag => (tag, 1)).updateStateByKey[Int](updateFunction _)



    ssc.start()
    ssc.awaitTermination()
  }

  def jsonToStreamLog(jsonStr: String): SteamLog = {
    val gson = new Gson()
    gson.fromJson(jsonStr, classOf[SteamLog])
  }

  def jsonToGameDetail(jsonStr: String): GameDetail = {
    val gson = new Gson()
    gson.fromJson(jsonStr, classOf[GameDetail])
  }

  /**
   * 把当前的数据去更新已有的或者是老的数据
   * @param currentValues  当前的
   * @param preValues  老的
   * @return
   */
  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)

    Some(current + pre)
  }
}
