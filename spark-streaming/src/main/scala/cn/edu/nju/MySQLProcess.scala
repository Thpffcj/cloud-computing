package cn.edu.nju

import java.text.SimpleDateFormat
import java.util

import cn.edu.nju.api.{ApiReturnObject, TagReturnObject}
import cn.edu.nju.domain.{GameObject, TagObject}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * Created by thpffcj on 2019/10/24.
 */
class MySQLProcess {

  /**
   * 返回动态图所需数据
   * @param dates
   * @return
   */
  def getTimeFieldData(dates: ListBuffer[String]): ApiReturnObject = {

    val sparkConf = new SparkConf().setMaster("local[1]").setAppName("MySQLProcess")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    val timeFieldObjects = new util.ArrayList[TimeFieldObject]

    for (date <- dates){
      val time = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date).getTime / 1000).toInt
      val tableName = "(select * from top10 where time = " + time + " order by recommendations_up desc limit 10) as top10"
      val data: DataFrame = readMysqlTable(sqlContext, tableName)

      val gameObjects = new util.ArrayList[GameObject]
      val broadcast = sc.broadcast(gameObjects)
      var id = 1
      data.foreach(row => {

        val name = row.getAs("name").toString
        var color = ""
        if (MySQLProcess.map.containsKey(name)) {
          color = MySQLProcess.map.get(name).toString
        } else {
          // rgb(218, 198, 76)
          color = "rgb(" + Random.nextInt(255) + ", " + Random.nextInt(255) + ", " + Random.nextInt(255) + ")"
          MySQLProcess.map.put(name, color)
        }

        val gameObject = new GameObject(id.toString, name, row.getAs("recommendations_up"), color)
        broadcast.value.add(gameObject)
        id = id + 1
      })

      val name = "截止" + date.substring(0, 4) + "年" + date.substring(5, 7) + "月" + "好评累计总数"
      val timeFieldObject = new TimeFieldObject(name, broadcast.value)
      timeFieldObjects.add(timeFieldObject)
    }

    val apiReturnObject = new ApiReturnObject(timeFieldObjects)

    sc.stop()

    apiReturnObject
  }

  /**
   * 返回词云需要的数据
   * @return
   */
  def getTagData(round: Int): TagReturnObject = {

    val sparkConf = new SparkConf().setMaster("local[1]").setAppName("MySQLProcess")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    val tableName = "(select * from tag limit " + 0 + "," + round * 50 + ") as top10"
    println(tableName)
    val data: DataFrame = readMysqlTable(sqlContext, tableName)

    val tagObjects =new util.ArrayList[TagObject]
    val broadcast = sc.broadcast(tagObjects)
    data.foreach(row => {
      val tagObject = new TagObject(row.getAs("game_name"), row.getAs("number"))
      broadcast.value.add(tagObject)
    })

    val tagReturnObject = new TagReturnObject(tagObjects)

    sc.stop()

    tagReturnObject
  }

  def readMysqlTable(sqlContext: SQLContext, tableName: String) = {
    sqlContext
      .read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://172.19.240.128:3306/steam")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", tableName)
      .load()
  }
}

object MySQLProcess {

  val map = new util.HashMap[String, String]()
}
