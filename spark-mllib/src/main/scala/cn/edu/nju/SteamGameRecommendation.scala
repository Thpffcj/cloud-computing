package cn.edu.nju

import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.SparkSession

/**
 * Created by thpffcj on 2019/11/16.
 */
object SteamGameRecommendation {

  def main(args: Array[String]): Unit = {

    test()
  }

  def train() = {

    val conf = new SparkConf().setMaster("local[4]").setAppName("SteamGameRecommendation")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val data = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/steam_rating.csv")
      .select("userId", "gameId", "gameName", "rating", "random")
      .sort("random")
      .select("userId", "gameId", "rating")

    val Array(train, test) = data.randomSplit(Array(0.7, 0.3))

    val als = new ALS()
      .setMaxIter(20)
      .setUserCol("userId")
      .setItemCol("gameId")
      .setRatingCol("rating")
      // 正则化参数
      .setRegParam(0.01)

    val model = als.fit(train)

    // 冷启动策略
    model.setColdStartStrategy("drop")

    val predictions = model.transform(test)
    // 根据(userId, gameId)预测rating
    predictions.show(false)

    // 模型评估
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")

    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error is $rmse \n")

    // Spark机器学习模型的持久化
    // 模型保存
    model.save("src/main/resources/model/game_recommendation.model")

    spark.stop()
  }

  def test() = {

    val conf = new SparkConf().setMaster("local").setAppName("SteamGameRecommendation")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // 模型加载
    val model = ALSModel.load("src/main/resources/model/game_recommendation.model")

    import spark.implicits._
    val users = spark.createDataset(Array(1)).toDF("userId")
    users.show(false)

    model.recommendForUserSubset(users, 5).show(false)

    spark.stop()
  }
}
