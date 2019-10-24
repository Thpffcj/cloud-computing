package cn.edu.nju

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
 * Created by thpffcj on 2019/10/19.
 */
object BatchProcess {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamProcess")

    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    val startDate = "2017-01-01 08:00:00"
    val startTime: Int = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(startDate).getTime / 1000).toInt

    val endDate = "2019-10-01 08:00:00"
    val endTime : Int = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(endDate).getTime / 1000).toInt

    for (time <- Range(startTime, endTime + 1, 2678400)){
      val tableName = "(select * from roll_up where time = " + time + " order by recommendations_up desc limit 10) as roll_up"
      val data: DataFrame = readMysqlTable(sqlContext, tableName)

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
