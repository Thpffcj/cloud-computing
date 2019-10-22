package cn.edu.nju

import java.sql.DriverManager

import cn.edu.nju.domain.CommentLog
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

    // 如果使用了stateful的算子，必须要设置checkpoint
    // 在生产环境中，建议把checkpoint设置到HDFS的某个文件夹中
    // . 代表当前目录
    ssc.checkpoint("/Users/thpffcj/Public/file/cloud_checkpoint/hdfs_process")

//    val data = ssc.textFileStream("hdfs://thpffcj:9000/cloud-computing/")
    // nc -lk 9999
    val data = ssc.socketTextStream("localhost", 9999)

    // 构建黑名单
    val blacks = List("9999")
    val blacksRDD = ssc.sparkContext.parallelize(blacks).map(x => (x, true))

    // 过滤黑名单
    val cleanData = data.map(line => (line.split(",")(0), line))
        .transform(rdd => {
          rdd.leftOuterJoin(blacksRDD)
            .filter(x => x._2._2.getOrElse(false) != true)
            .map(x => x._2._1)
        })

    val logs = cleanData.map(line => {
      val infos = line.split(",")
      CommentLog(infos(0), infos(1), infos(2), infos(3))
    }).filter(commentLog => commentLog.gameName != "")

    // 按游戏名统计评论数
    val gameNumber = logs.map(log => {
      (log.gameName, 1)
    }).updateStateByKey[Int](updateFunction _)

    gameNumber.print()

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 把当前的数据去更新已有的或者是老的数据
   * @param currentValues  当前的
   * @param preValues  老的
   * @return
   */
  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)

    Some(current + pre)
  }

  /**
   * 获取MySQL的连接
   */
  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/spark", "root", "000000")
  }
}
