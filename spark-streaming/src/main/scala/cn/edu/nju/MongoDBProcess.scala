package cn.edu.nju

import com.mongodb.spark.MongoSpark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame

/**
 * Created by thpffcj on 2019/9/24.
 */
object MongoDBProcess {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("MongoDBProcess")
      .config("spark.mongodb.input.uri", "mongodb://steam:steam@***.***.***.***:27017/steam_db.China.games")
      .getOrCreate()

    val frame: DataFrame = MongoSpark.load(spark)
    frame.createTempView("games")

    val res: DataFrame = spark.sql("SELECT name from games")
    res.show()
  }
}
