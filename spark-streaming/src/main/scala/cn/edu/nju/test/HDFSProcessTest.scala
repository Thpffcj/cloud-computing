package cn.edu.nju.test

import cn.edu.nju.domain.{CommentLog, DouBanLog}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by thpffcj on 2019/10/2.
 */
object HDFSProcessTest {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("HDFSProcess")

    // 创建StreamingContext需要两个参数：SparkConf和batch interval
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val data = ssc.textFileStream("hdfs://thpffcj:9000/cloud-computing/")

    val log = data.map(line => {

      val infos = line.split("\t")

      DouBanLog(infos(0).toDouble, infos(1), infos(2), infos(3))
    })

    log.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
