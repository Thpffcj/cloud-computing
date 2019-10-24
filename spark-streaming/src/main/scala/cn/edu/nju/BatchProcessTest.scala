package cn.edu.nju

import java.util
import java.util.ArrayList

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by thpffcj on 2019/10/24.
 */
class BatchProcessTest {

  def getTimeFieldData(startTime: Int, endTime: Int): ApiReturnObject = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("BatchProcessTest")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    val timeFieldObjects = new util.ArrayList[TimeFieldObject]
    for (time <- Range(startTime, endTime + 1, 2678400)){
      val tableName = "(select * from top10 where time = " + time + " order by recommendations_up desc limit 10) as top10"
      val data: DataFrame = readMysqlTable(sqlContext, tableName)

      val gameObjects = new util.ArrayList[GameObject]
      val broadcast = sc.broadcast(gameObjects)
      data.foreach(row => {
        val gameObject = new GameObject("id", row.getAs("name"), row.getAs("recommendations_up"), "color")
        broadcast.value.add(gameObject)
      })
      val timeFieldObject = new TimeFieldObject(time.toString, broadcast.value)
      timeFieldObjects.add(timeFieldObject)
    }

    val apiReturnObject = new ApiReturnObject(timeFieldObjects)

    apiReturnObject
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
