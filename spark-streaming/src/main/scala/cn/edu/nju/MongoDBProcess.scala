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
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.doubanmovies")
      .getOrCreate()
    val frame: DataFrame = MongoSpark.load(spark)
    frame.createTempView("movies")
    val res: DataFrame = spark.sql("SELECT star, bd, quote, title from movies")
    res.show()
  }
}
