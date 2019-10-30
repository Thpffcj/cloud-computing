package cn.edu.nju

import java.text.SimpleDateFormat
import java.util.Properties

import cn.edu.nju.utils.DateUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer


/**
 * Created by thpffcj on 2019/10/19.
 */
object BatchProcess {

  def main(args: Array[String]): Unit = {
    saveTop10ToCsv()
  }

  /**
   * 从MySQL中读取top10写入csv文件
   */
  def saveTop10ToCsv(): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("BatchProcess")
    val sc = SparkSession.builder().config(sparkConf).getOrCreate()

    val csvSavePath = "src/main/resources/RollupCSV"

    val tableName = "(select name, recommendations_up, time from top10_new order by time) as top10"
    val data: DataFrame = readMysqlTable(sc, tableName)

    import sc.implicits._
    data.map(row => {

      val name = row.getAs("name").toString
      val types = "game"
      val recommendations_up = row.getAs("recommendations_up").toString
      val date = DateUtils.tranTimestampToString(row.getAs("time"))

      println((name, types, recommendations_up, date))

      (name, types, recommendations_up, date)
    }).toDF("name", "type", "value", "date").write.mode(SaveMode.Overwrite).csv(csvSavePath)

    sc.stop()
  }

  /**
   * 按月份统计top10存入MySQL
   */
  def saveRollUpToMysql() = {

    val sparkConf = new SparkConf().setMaster("local[1]").setAppName("BatchProcess")
    val sc = SparkSession.builder().config(sparkConf).getOrCreate()

    val dates = DateUtils.getSteamDates()

    for (date <- dates) {
      val time = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date).getTime / 1000).toInt
      println(time)
      val tableName = "(select * from roll_up where time = " + time + " order by recommendations_up desc limit 10) as roll_up"
      val data: DataFrame = readMysqlTable(sc, tableName)

      val properties = new Properties()
      properties.setProperty("user", "root")
      properties.setProperty("password", "root")
      data.write.mode(SaveMode.Append).jdbc("jdbc:mysql://172.19.240.128:3306/steam", "top10_new", properties)
    }

    sc.stop()
  }

  // TODO Spark不支持Update操作
  def addRollUpByMonth()= {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("BatchProcess")
    val sc = SparkSession.builder().config(sparkConf).getOrCreate()
    val dates = DateUtils.getSteamDates()

    var tableName = "(select name from roll_up) as roll_up"
    val data: DataFrame = readMysqlTable(sc, tableName)

    // 广播变量
    val gameName = new ListBuffer[String]
    val broadcast: Broadcast[ListBuffer[String]] = sc.sparkContext.broadcast(gameName)
    data.foreach(row => {
      broadcast.value.append(row.getAs("name").toString)
    })

    for (game <- gameName) {
      tableName = "(select recommendations_up from roll_up where name = '" + game + "') as roll_up"
      val data: DataFrame = readMysqlTable(sc, tableName)
      data.show()
    }

    sc.stop()
  }

  def readMysqlTable(sparkSession: SparkSession, tableName: String) = {

    sparkSession
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://172.19.240.128:3306/steam")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", tableName)
      .load()
  }
}
