package cn.edu.nju

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Created by thpffcj on 2019/10/19.
 */
object BatchProcess {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamProcess")

    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    val data: DataFrame = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true") //在csv第一行有属性"true"，没有就是"false"
      .option("inferSchema", true.toString) //这是自动推断属性列的数据类型
      .load("/Users/thpffcj/Public/file/steam.csv")

    data.show()

  }
}
