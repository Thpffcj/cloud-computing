package cn.edu.nju.test

import java.util

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import steamserverdemo.{ApiReturnObject, GameObject, TimeFieldObject}

/**
 * Created by thpffcj on 2019/10/24.
 */
class BatchProcessTest {

  def getTimeFieldData():ApiReturnObject = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamProcess")

    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    val time = 1398902400
    val tableName = "(select * from top10 where time = " + time + " order by recommendations_up desc limit 10) as roll_up"
    val data: DataFrame = readMysqlTable(sqlContext, tableName)

    val gameObject = new GameObject("id", "label", 1, "color")
    val gameObjects = new util.ArrayList[GameObject]
    gameObjects.add(gameObject)

    val timeFieldObject = new TimeFieldObject("2017年10月", gameObjects)
    val timeFieldObjects = new util.ArrayList[TimeFieldObject]
    timeFieldObjects.add(timeFieldObject)

    new ApiReturnObject(timeFieldObjects)
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
