package cn.edu.nju

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by thpffcj on 2019/10/2.
 */
object HDFSProcess {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("HDFSProcess")
//    val sparkConf = new SparkConf().setMaster("spark://thpffcj:7077").setAppName("HDFSProcess")

    // 创建StreamingContext需要两个参数：SparkConf和batch interval
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.textFileStream("hdfs://thpffcj:9000/cloud-computing/")

    lines.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
