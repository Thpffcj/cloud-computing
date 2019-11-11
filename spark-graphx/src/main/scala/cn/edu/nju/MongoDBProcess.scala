package cn.edu.nju

import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.sql._
import org.apache.spark.sql.SparkSession

/**
 * Created by thpffcj on 2019/10/31.
 */
object MongoDBProcess {

  def main(args: Array[String]): Unit = {

    val MongoUri1 = args(0).toString
    val MongoUri2 = args(1).toString
    val SparkMasterUri = args(2).toString

    def makeMongoURI(uri: String, database: String, collection: String) = (s"${uri}/${database}.${collection}")

    val mongoURI1 = s"mongodb://${MongoUri1}:27017"
    val mongoURI2 = s"mongodb://${MongoUri2}:27017"

    val CONFdb1 = makeMongoURI(s"${mongoURI1}", "MyColletion1", "df")
    val CONFdb2 = makeMongoURI(s"${mongoURI2}", "MyColletion2", "df")

    val WRITEdb1: WriteConfig = WriteConfig(scala.collection.immutable.Map("uri" -> CONFdb1))
    val READdb1: ReadConfig = ReadConfig(Map("uri" -> CONFdb1))

    val WRITEdb2: WriteConfig = WriteConfig(scala.collection.immutable.Map("uri" -> CONFdb2))
    val READdb2: ReadConfig = ReadConfig(Map("uri" -> CONFdb2))

    val spark = SparkSession
      .builder
      .appName("AppMongo")
      .config("spark.worker.cleanup.enabled", "true")
      .config("spark.scheduler.mode", "FAIR")
      .getOrCreate()

    val df1 = spark.read.mongo(READdb1)
    val df2 = spark.read.mongo(READdb2)
    df1.write.mode("overwrite").mongo(WRITEdb1)
    df2.write.mode("overwrite").mongo(WRITEdb2)
  }
}

