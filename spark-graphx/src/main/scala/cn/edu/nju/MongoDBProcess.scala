package cn.edu.nju

import com.mongodb.spark.MongoSpark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Created by thpffcj on 2019/10/31.
 */
object MongoDBProcess {

//  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("MongoDBProcess")
    conf.set("spark.mongodb.input.uri", "mongodb://steam:steam@101.132.176.87:27017/steam_db.China.games")
    conf.set("spark.mongodb.input.partitioner","MongoShardedPartitioner")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val frame: DataFrame = MongoSpark.load(spark)
    frame.show()
//    frame.createTempView("reviews")
//
//    val res: DataFrame = spark.sql("SELECT name from reviews")
//    res.show()
  }
}

