package cn.edu.nju

import cn.edu.nju.dao.{RollUpDAO, TagDAO}
import cn.edu.nju.domain.{GameDetail, ReviewsChart, RollUp, SteamLog, Tag}
import com.google.gson.Gson
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.{ListBuffer, Set}

/**
 * Created by thpffcj on 2019/10/21.
 */
object SteamProcess {

  def main(args: Array[String]): Unit = {

//    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("HDFSProcess")
    val sparkConf = new SparkConf().setMaster("spark://thpffcj:7077").setAppName("SteamProcess")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 如果使用了stateful的算子，必须要设置checkpoint
    // 在生产环境中，建议把checkpoint设置到HDFS的某个文件夹中
    // . 代表当前目录
    ssc.checkpoint("/Users/thpffcj/Public/file/cloud_checkpoint/hdfs_process")

    val rawData = ssc.textFileStream("hdfs://thpffcj:9000/cloud-computing/")

    val gameNameSet: Set[String] = Set()

    /**
     * 过滤空行
     * 过滤日期为空的数据
     * 过滤重复的数据，使用游戏名称过滤
     * 过滤game_detail为空的数据
     * 为了game_detail为bundle的数据
     */
    val data = rawData.filter(rdd => !rdd.isEmpty).map(line => {
      val log = line.split("\t")
      if (log.length < 7) {
        SteamLog("", "", "", "", "", "", "")
      } else {
        SteamLog(log(0), log(1), log(2), log(3), log(4), log(5), log(6))
      }
    }).filter(steamLog => !steamLog.date.isEmpty)
      .filter(steamLog => !gameNameSet.contains(steamLog.name))
      .filter(steamLog => !steamLog.game_detail.isEmpty)
      .filter(steamLog => !steamLog.game_detail.equals("bundle"))
      .map(steamLog => {
        gameNameSet.add(steamLog.name)
        steamLog
      })

    // 取出用户标签
    val userTags = data.map(steamLog => {
      val gameDetail = jsonToGameDetail(steamLog.game_detail)
      if (gameDetail != null) {
        gameDetail.user_tags.toString.replace(" ", "")
      } else {
        null
      }
    }).filter(userTags => userTags != null)

    // 标签统计
    val tagsNumber = userTags.flatMap(line => line.substring(1, line.length - 1).split(","))
      .map(tag => (tag, 1)).updateStateByKey[Int](updateFunction _)

//    writeTagToMysql(tagsNumber)
    tagsNumber.print()

    /**
     * (steamLog.name,jsonToReviewsChart(gameDetail.reviewsChart.toString))
     * (CODE VEIN,{recommendations_down=34.0,date=1.5712704E9,recommendations_up=167.0},{recommendations_down=34.0,date=1.5712704E9,recommendations_up=167.0)
     */
    val rollups = data.map(steamLog => {
      val gameDetail = jsonToGameDetail(steamLog.game_detail)

      // 过滤 reviewsChart["start_date"] 和 reviewsChart["end_date"] 为空的数据
      if ((gameDetail != null) && (gameDetail.reviewsChart.get("start_date") != "None")
        && (gameDetail.reviewsChart.get("end_date") != "None")) {
        (steamLog.name, jsonToReviewsChart(gameDetail.reviewsChart.toString))
      } else {
        null
      }
    }).filter(rollups => rollups != null)
      // 目前只考虑以月为时间单位的数据
      .filter(reviewsChart => reviewsChart._2.rollup_type == "month")
      .map(reviewsChart => {
        val line = reviewsChart._2.rollups.toString
        (reviewsChart._1, line.substring(1, line.length - 2).replace(" ", ""))
      })

    // 将每个游戏好评数写入到MySQL
    rollups.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[(String, Int, Int, Int)]
        
        partitionOfRecords.foreach(record => {
          record._2.split("},").foreach(data => {
            val rollUp = jsonToRollUp(data + "}")
            list.append((record._1, rollUp.date, rollUp.recommendations_up, rollUp.recommendations_down))
          })
        })

        RollUpDAO.insertRollUp(list)
      })
    })

    // 单条插入
//    rollups.foreachRDD(rdd => {
//      rdd.foreachPartition(partitionOfRecords => {
//        val connection = createConnection()
//        partitionOfRecords.foreach(record => {
//          record._2.split("},").foreach(data => {
//            val rollUp = jsonToRollUp(data + "}")
//            val sql = "insert into roll_up(name, time, recommendations_up, recommendations_down) values('" + record._1.replace("'", "") + "'," + rollUp.date + "," + rollUp.recommendations_up + "," + rollUp.recommendations_down + ")"
//            connection.createStatement().execute(sql)
//          })
//        })
//        connection.close()
//      })
//    })

    rollups.print()

    ssc.start()
    ssc.awaitTermination()
  }

//  def createConnection() = {
//    Class.forName("com.mysql.jdbc.Driver")
//    DriverManager.getConnection("jdbc:mysql://localhost:3306/steam?useUnicode=true&characterEncoding=utf-8", "root", "000000")
//  }

  def jsonToGameDetail(jsonStr: String): GameDetail = {
    try {
      val gson = new Gson()
      gson.fromJson(jsonStr, classOf[GameDetail])
    } catch {
      case e: Exception => {
//        e.printStackTrace()
        null
      }
    }
  }

  def jsonToReviewsChart(jsonStr: String): ReviewsChart = {
    try {
      val gson = new Gson()
      gson.fromJson(jsonStr, classOf[ReviewsChart])
    } catch {
      case e: Exception => {
//        e.printStackTrace()
        null
      }
    }
  }

  def jsonToRollUp(jsonStr: String): RollUp = {
    try {
      val gson = new Gson()
      gson.fromJson(jsonStr, classOf[RollUp])
    } catch {
      case e: Exception => {
//        e.printStackTrace()
        null
      }
    }
  }

  /**
   * 把当前的数据去更新已有的或者是旧的数据
   *
   * @param currentValues 当前数据
   * @param preValues     旧数据
   * @return
   */
  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)
    Some(current + pre)
  }

  /**
   * 标签数据写入MySQL
   * @param tagsNumber
   */
  def writeTagToMysql(tagsNumber: DStream[(String, Int)]): Unit = {

    tagsNumber.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[Tag]
        partitionOfRecords.foreach(record => {
          list.append(Tag(record._1, record._2))
        })
        TagDAO.insertTag(list)
      })
    })
  }
}
