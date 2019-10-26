package cn.edu.nju.test

import cn.edu.nju.dao.{CourseClickCountDAO, CourseSearchClickCountDAO}
import cn.edu.nju.domain.{ClickLog, CourseClickCount, CourseSearchClickCount}
import cn.edu.nju.utils.DateUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
 * Created by thpffcj on 2019/10/17.
 */
object StatStreaming {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("StatStreaming") //.setMaster("local[5]")
    val ssc = new StreamingContext(sparkConf, Seconds(60))

    val bootstrapServers = "thpffcj1:9092"
    val groupId = "test"
    val topicName = "test"
    val maxPoll = 20000

    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> maxPoll.toString,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val messages = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(topicName), kafkaParams))

    // 测试步骤一：测试数据接收
    // messages.map(_._2).count().print

    // 测试步骤二：数据清洗
    val logs = messages.map(_.value())
    val cleanData = logs.map(line => {
      val infos = line.split("\t")

      // infos(2) = "GET /class/130.html HTTP/1.1"
      // url = /class/130.html
      val url = infos(2).split(" ")(1)
      var courseId = 0

      // 把实战课程的课程编号拿到了
      if (url.startsWith("/class")) {
        val courseIdHTML = url.split("/")(2)
        courseId = courseIdHTML.substring(0, courseIdHTML.lastIndexOf(".")).toInt
      }

      ClickLog(infos(0), DateUtils.parseToMinute(infos(1)), courseId, infos(3).toInt, infos(4))
    }).filter(clicklog => clicklog.courseId != 0)

    //    cleanData.print()

    // 测试步骤三：统计今天到现在为止实战课程的访问量

    cleanData.map(x => {

      // HBase rowkey设计： 20171111_88
      (x.time.substring(0, 8) + "_" + x.courseId, 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[CourseClickCount]

        partitionRecords.foreach(pair => {
          list.append(CourseClickCount(pair._1, pair._2))
        })

        CourseClickCountDAO.save(list)
      })
    })

    // 测试步骤四：统计从搜索引擎过来的今天到现在为止实战课程的访问量

    cleanData.map(x => {

      /**
       * https://www.sogou.com/web?query=Spark SQL实战
       */
      val referrer = x.referrer.replaceAll("//", "/")
      val splits = referrer.split("/")
      var host = ""
      if(splits.length > 2) {
        host = splits(1)
      }

      (host, x.courseId, x.time)
    }).filter(_._1 != "").map(x => {
      (x._3.substring(0,8) + "_" + x._1 + "_" + x._2 , 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[CourseSearchClickCount]

        partitionRecords.foreach(pair => {
          list.append(CourseSearchClickCount(pair._1, pair._2))
        })

        CourseSearchClickCountDAO.save(list)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
