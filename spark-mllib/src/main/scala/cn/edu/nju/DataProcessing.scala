package cn.edu.nju

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Random

/**
 * Created by thpffcj on 2019/11/18.
 */
object DataProcessing {

  def main(args: Array[String]): Unit = {

    getStreamRating()
  }

  def getStreamRating() = {

    val gameMap = new util.HashMap[String, Int]()
    // 游戏出现次数
    val gameNumber = new util.HashMap[String, Int]()
    val maxTimeMap = new util.HashMap[String, Double]

    val conf = new SparkConf().setMaster("local").setAppName("DataProcessing")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    var data = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/steam.csv")
      .select("userId", "gameName", "behavior", "duration", "gameId")

    data = data.filter(row => row.getAs("behavior").equals("play"))

    var key = 1
    data.collect().foreach(row => {

      val gameName = row.getAs("gameName").toString
      val duration = row.getAs("duration").toString.toDouble

      if (!gameMap.containsKey(gameName)) {
        gameMap.put(gameName, key)
        key = key + 1
      }

      if (gameNumber.containsKey(gameName)) {
        gameNumber.put(gameName, gameNumber.get(gameName) + 1)
      } else {
        gameNumber.put(gameName, 1)
      }

      if (maxTimeMap.containsKey(gameName)) {
        if (duration > maxTimeMap.get(gameName)) {
          maxTimeMap.put(gameName, duration)
        }
      } else {
        maxTimeMap.put(gameName, duration)
      }

    })

    import spark.implicits._
    val rand = new Random()
    val cleanData = data.filter(row => {
      gameNumber.get(row.getAs("gameName").toString) > 2
    }).map(row => {

      val userId = row.getAs("userId").toString
      val gameName = row.getAs("gameName").toString
      var duration = (row.getAs("duration").toString.toDouble / maxTimeMap.get(gameName) * 10).formatted("%.2f")
      if (duration.toDouble < 1.0) {
        duration = "1.0"
      }
      val gameId = gameMap.get(gameName)
      val random = rand.nextDouble()

      (userId, gameId, gameName, duration, random)
    })

    cleanData.repartition(1).write.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("delimiter", ",")
      .mode(SaveMode.Overwrite)
      .save("src/main/resources/steam_rating.csv")

    spark.stop()
  }
}
