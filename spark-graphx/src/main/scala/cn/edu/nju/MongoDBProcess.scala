package cn.edu.nju

import com.mongodb.spark.MongoSpark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document

/**
 * Created by thpffcj on 2019/10/31.
 */
object MongoDBProcess {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("MongoDBProcess")
    conf.set("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.China.reviews")
    conf.set("spark.mongodb.input.partitioner","MongoPaginateBySizePartitioner")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val frame: DataFrame = MongoSpark.load(spark)

    frame.foreach(row => {
      val jsonPlayer = row.getAs("user").toString.split(",")
      val player = jsonPlayer(1).substring(0, jsonPlayer(1).length - 1)

      val jsonGame = row.getAs("game").toString.split(",")
      val game = jsonGame(0).substring(1)

      val content = row.getAs("content").toString

//      println(player + " " + game + " " + content)

    })

//    writeToMongodb(frame)

//    frame.foreach(row => {
//
//    })
//    frame.createTempView("reviews")
//
//    val res: DataFrame = spark.sql("SELECT name from reviews")
//    res.show()
  }

  /**
   * 从MongoDB中读取数据
   */
  def readFromMongodb(): DataFrame = {

    val conf = new SparkConf().setMaster("local[4]").setAppName("MongoDBProcess")
    conf.set("spark.mongodb.input.uri", "mongodb://steam:steam@101.132.176.87:27017/steam_db.China.reviews")
    conf.set("spark.mongodb.input.partitioner","MongoPaginateBySizePartitioner")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val frame: DataFrame = MongoSpark.load(spark)

    frame.show()

//    frame.createTempView("reviews")
//    val limit10: DataFrame = spark.sql("SELECT * from reviews limit 10")

    spark.stop()

    frame
  }

  /**
   * 数据写入MongoDB
   * @param df
   */
  def writeToMongodb(df: DataFrame) = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoDBProcess")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.China.reviews")
      .getOrCreate()

    // 设置log级别
    spark.sparkContext.setLogLevel("WARN")

//    val document1 = new Document()
//    document1.append("name", "sunshangxiang").append("age", 18).append("sex", "female")
//    val document2 = new Document()
//    document2.append("name", "diaochan").append("age", 24).append("sex", "female")
//    val document3 = new Document()
//    document3.append("name", "huangyueying").append("age", 23).append("sex", "female")
//
//    val seq = Seq(document1, document2, document3)
//    val df = spark.sparkContext.parallelize(seq)


    val data = spark.sparkContext.parallelize(Seq(df))

    // 将数据写入mongo
    MongoSpark.save(data)

    spark.stop()
  }

}

