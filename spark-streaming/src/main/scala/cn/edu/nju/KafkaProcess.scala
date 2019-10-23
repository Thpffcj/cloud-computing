package cn.edu.nju

import cn.edu.nju.domain.UserData
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by thpffcj on 2019/10/19.
 */
object KafkaProcess {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamProcess")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.checkpoint("/Users/thpffcj/Public/file/cloud_checkpoint/stream_process")

    val zkQuorum = "thpffcj1:2181"
    val topicMap = Map("steam" -> 1)
    val groupId = "test"
    val messages = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap)

    val rawData = messages.map(_._2)

    val data = rawData.map(line => {
      val record = line.split("\t")
      UserData(record(0), record(1), record(2), record(3).toDouble)
    })

    // 游戏销量
    val gameSale = data.filter(userData => userData.behavior == "purchase")
      .map(userData => {
        (userData.gameName, 1)
      }).updateStateByKey[Int](updateFunction _)

    gameSale.print()

    // 游戏游玩平均时长
    val gamePopularity = data.filter(userData => userData.behavior == "play").map(
      userData => {
        (userData.gameName, (userData.duration, 1))
      }
    ).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    gamePopularity.print()

    // Dota 2游玩时长
    val gameDuration = data.filter(
      userData => userData.gameName == "Dota 2" & userData.behavior == "play").map(
      userData => {
        (userData.userId, userData.duration)
      })

    gameDuration.print()

    ssc.start()
    ssc.awaitTermination()
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

//  def addGamePopularity(currentValues: Seq[(Int, Int)], preValues: Option[(Int, Int)]): Option[(Int, Int)] = {
//    val currentSum = currentValues.map(current => current._1).sum
//    val currentNumber = currentValues.map(current => current._2).sum
//
//    val preSum = preValues.map(current => current._1).getOrElse(0)
//    val preNumber = preValues.map(current => current._2).getOrElse(0)
//
//    Some((currentSum + preSum, currentNumber + preNumber))
//  }
}
