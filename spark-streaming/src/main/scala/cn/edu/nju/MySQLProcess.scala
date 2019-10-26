package cn.edu.nju

import java.text.SimpleDateFormat
import java.util
import java.util.ArrayList

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * Created by thpffcj on 2019/10/24.
 */
class MySQLProcess {

  val sparkConf = new SparkConf().setMaster("local[1]").setAppName("MySQLProcess")
  val sc = new SparkContext(sparkConf)

  val sqlContext = new SQLContext(sc)

  def getTimeFieldData(dates: ListBuffer[String]): ApiReturnObject = {


    val timeFieldObjects = new util.ArrayList[TimeFieldObject]

    for (date <- dates){
      val time = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date).getTime / 1000).toInt
      val tableName = "(select * from top10 where time = " + time + " order by recommendations_up desc limit 10) as top10"
      val data: DataFrame = readMysqlTable(sqlContext, tableName)

      val gameObjects = new util.ArrayList[GameObject]
      val broadcast = sc.broadcast(gameObjects)
      data.foreach(row => {
        val gameObject = new GameObject("id", row.getAs("name"), row.getAs("recommendations_up"), "color")
        broadcast.value.add(gameObject)
      })

      val timeFieldObject = new TimeFieldObject(date, broadcast.value)
      timeFieldObjects.add(timeFieldObject)
    }

    val apiReturnObject = new ApiReturnObject(timeFieldObjects)

    apiReturnObject
  }

  /**
   * 返回词云需要的数据
   * @return
   */
  def getTagData(): TagReturnObject = {

    val tableName = "tag"
    val data: DataFrame = readMysqlTable(sqlContext, tableName)

    val tagObjects =new util.ArrayList[TagObject]
    val broadcast = sc.broadcast(tagObjects)
    data.foreach(row => {
      val tagObject = new TagObject(row.getAs("game_name"), row.getAs("number"))
      broadcast.value.add(tagObject)
    })

    val tagReturnObject = new TagReturnObject(tagObjects)
    tagReturnObject
  }

  def readMysqlTable(sqlContext: SQLContext, tableName: String) = {
    sqlContext
      .read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/steam")
      .option("user", "root")
      .option("password", "000000")
      .option("dbtable", tableName)
      .load()
  }
}
