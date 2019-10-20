package cn.edu.nju

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by thpffcj on 2019/10/19.
 */
object StreamProcess {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamProcess")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

//    val rawData = ssc.socketTextStream("localhost", 9999)

    val zkQuorum = "thpffcj:2181"
    val topicMap = Map("steam" -> 1)
    val messages = KafkaUtils.createStream(ssc, zkQuorum, "test", topicMap)

    messages.map(_._2).print

//    val data = rawData.map(line => {
//      val record = line.split(",")
//      // 去除引号
//      val gameName = record(1).substring(1, record(1).length - 1)
//      UserData(record(0), gameName, record(2), record(3).toDouble)
//    })
//
//    // 游戏销量
//    val gameSale = data.filter(userData => userData.behavior == "purchase")
//      .map(userData => {
//        (userData.gameName, 1)
//      }).reduceByKey(_ + _)
//
//    // 游戏游玩平均时长
//    val gamePopularity = data.filter(userData => userData.behavior == "play").map(
//      userData => {
//        (userData.gameName, (userData.duration, 1))
//      }
//    ).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
//
//    // Dota 2游玩时长
//    val gameDuration = data.filter(
//      userData => userData.gameName == "Dota 2" & userData.behavior == "play").map(
//      userData => {
//        (userData.userId, userData.duration)
//      }).reduceByKey(_ + _)

//    gameDuration.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
