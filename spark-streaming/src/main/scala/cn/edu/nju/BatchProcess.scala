package cn.edu.nju

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Properties

import cn.edu.nju.utils.DateUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.collection.mutable.ListBuffer

/**
 * Created by thpffcj on 2019/10/19.
 */
object BatchProcess {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[1]").setAppName("StreamProcess")

    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    val dates = DateUtils.getSteamDates()

    for (date <- dates){
      val time = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date).getTime / 1000).toInt
      println(time)
      val tableName = "(select * from roll_up where time = " + time + " order by recommendations_up desc limit 10) as roll_up"
      val data: DataFrame = readMysqlTable(sqlContext, tableName)
//      Don't Starve Together: All Snowfallen Feast Chest
//      Don't Starve Together: All Snowfallen Feast Chest
//      Don't Starve Together: All Survivors Gladiator Chest

      val properties = new Properties()
      properties.setProperty("user", "root")
      properties.setProperty("password", "000000")
      data.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/steam", "top10", properties)
    }

    sc.stop()
  }

  def readMysqlTable(sqlContext: SQLContext, tableName: String) = {
    sqlContext
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/steam")
      .option("user", "root")
      .option("password", "000000")
      .option("dbtable", tableName)
      .load()
  }
}
