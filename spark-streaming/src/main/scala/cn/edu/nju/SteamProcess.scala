package cn.edu.nju

import java.sql.DriverManager

import cn.edu.nju.domain.{GameDetail, ReviewsChart, RollUp, SteamLog}
import com.google.gson.Gson
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.mutable.Set

/**
 * Created by thpffcj on 2019/10/21.
 */
object SteamProcess {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("HDFSProcess")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.checkpoint("/Users/thpffcj/Public/file/cloud_checkpoint/hdfs_process")

//    val rawData = ssc.socketTextStream("localhost", 9999)
    val rawData = ssc.textFileStream("hdfs://thpffcj:9000/cloud-computing/")


    val gameNameSet: Set[String] = Set()

    val data = rawData.filter(rdd => !rdd.isEmpty).map(line=> {
      val log = line.split("\t")
      SteamLog(log(0), log(1), log(2), log(3), log(4), log(5),log(6))
    }).filter(steamLog => !steamLog.date.isEmpty)
      .filter(steamLog => !gameNameSet.contains(steamLog.name))
      .filter(steamLog => !steamLog.game_detail.isEmpty)
      .filter(steamLog => !steamLog.game_detail.equals("bundle"))
      .map(steamLog => {
        gameNameSet.add(steamLog.name)
        steamLog
      })


    // 取出用户标签
//    val userTags = data.map(steamLog => {
//      val gameDetail = jsonToGameDetail(steamLog.game_detail)
//      gameDetail.user_tags.toString.replace(" ", "")
//    })

//
//    // 标签统计
//    val tagsNumber = userTags.flatMap(line => line.substring(1, line.length - 1).split(","))
//        .map(tag => (tag, 1)).updateStateByKey[Int](updateFunction _)

//    tagsNumber.print()

    /**
     * (CODE VEIN,{recommendations_down=34.0,date=1.5712704E9,recommendations_up=167.0},{recommendations_down=34.0,date=1.5712704E9,recommendations_up=167.0)
     */

    val rollups = data.map(steamLog => {
      println(steamLog)
      val gameDetail = jsonToGameDetail(steamLog.game_detail)
      (steamLog.name, jsonToReviewsChart(gameDetail.reviewsChart.toString))
    }).filter(reviewsChart => reviewsChart._2.rollup_type == "month")
      .map(reviewsChart => {
        val line = reviewsChart._2.rollups.toString
        (reviewsChart._1, line.substring(1, line.length - 2).replace(" ", ""))
    })

    // 将结果写入到MySQL
    // TODO 会有重复的数据
    rollups.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val connection = createConnection()
        partitionOfRecords.foreach(record => {
          record._2.split("},").foreach(data => {
            val rollUp = jsonToRollUp(data + "}")
            val sql = "insert into roll_up(name, time, recommendations_up, recommendations_down) values('" + record._1.replace("'", "") + "'," + rollUp.date + "," + rollUp.recommendations_up + "," + rollUp.recommendations_down + ")"
//            println(sql)
            connection.createStatement().execute(sql)
          })
        })
        connection.close()
      })
    })

    rollups.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def jsonToGameDetail(jsonStr: String): GameDetail = {
//    try {
      val gson = new Gson()
      gson.fromJson(jsonStr, classOf[GameDetail])
//    } catch {
//      case e: Exception => {
//        e.printStackTrace()
//        println("found io exception...")
//        println(jsonStr)
//
//        // TODO 怎么返回空啊
//        val gson = new Gson()
//        gson.fromJson(jsonStr, classOf[GameDetail])
//      }
//    }
  }

  def jsonToReviewsChart(jsonStr: String): ReviewsChart = {
//    try {
      val gson = new Gson()
      gson.fromJson(jsonStr, classOf[ReviewsChart])
//    } catch {
//      case e: Exception => {
//        e.printStackTrace()
//        println("found io exception...")
//        println(jsonStr)
//
//        // TODO 怎么返回空啊
//        val gson = new Gson()
//        gson.fromJson(jsonStr, classOf[ReviewsChart])
//      }
//    }
  }

  def jsonToRollUp(jsonStr: String): RollUp = {
    val gson = new Gson()
    gson.fromJson(jsonStr, classOf[RollUp])
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

  /**
   * 获取MySQL的连接
   */
  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/steam", "root", "000000")
  }
}
