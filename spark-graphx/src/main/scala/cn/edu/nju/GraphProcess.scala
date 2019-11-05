package cn.edu.nju

import java.util

import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Created by thpffcj on 2019/11/2.
 */
object GraphProcess {

  val pointMap = new util.HashMap[String, Long]()
  val edgeMap = new util.HashMap[(Long, Long), String]()

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("MongoDBProcess")
    conf.set("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.China.reviews")
    conf.set("spark.mongodb.input.partitioner","MongoPaginateBySizePartitioner")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val frame: DataFrame = MongoSpark.load(spark)

//    var vertexArray = Seq((1L, ("Alice", 28)))
//    vertexArray = vertexArray :+ (2L, ("Bob", 27))
//    vertexArray = vertexArray :+ (3L, ("Charlie", 65))
//    vertexArray = vertexArray :+ (4L, ("David", 42))
//    vertexArray = vertexArray :+ (5L, ("Ed", 55))
//    vertexArray = vertexArray :+ (6L, ("Fran", 50))
//
//    var edgeArray = Seq(Edge(2L, 1L, 7))
//    edgeArray = edgeArray :+ Edge(2L, 4L, 2)
//    edgeArray = edgeArray :+ Edge(3L, 2L, 4)
//    edgeArray = edgeArray :+ Edge(3L, 6L, 3)
//    edgeArray = edgeArray :+ Edge(4L, 1L, 1)
//    edgeArray = edgeArray :+ Edge(5L, 2L, 2)
//    edgeArray = edgeArray :+ Edge(5L, 3L, 8)
//    edgeArray = edgeArray :+ Edge(5L, 6L, 3)

    var key = 0L

    frame.foreach(row => {

      val jsonPlayer = row.getAs("user").toString.split(",")
      val player = jsonPlayer(1).substring(0, jsonPlayer(1).length - 1)

      val jsonGame = row.getAs("game").toString.split(",")
      val game = jsonGame(0).substring(1)

      val content = row.getAs("content").toString

      // 玩家点
      val playerKey = "user_" + player
      var playerPoint = 0L
      if (pointMap.containsKey(playerKey)) {
        playerPoint = pointMap.get(playerKey)
      } else {
        key = key + 1
        playerPoint = key
        pointMap.put(playerKey, playerPoint)
      }

      // 游戏点
      val gameKey = "game_" + game
      var gamePoint = 0L
      if (pointMap.containsKey(gameKey)) {
        gamePoint = pointMap.get(gameKey)
      } else {
        key = key + 1
        gamePoint = key
        pointMap.put(gameKey, gamePoint)
      }

      edgeMap.put((playerPoint, gamePoint), content)

      // KurokaneSS CODE VEIN 带妹子也就图一乐,打架还得靠云哥
//      println(player + " " + game + " " + content)
    })

//    println(pointMap)

    var vertexArray = Seq((0L, ("test", "test")))
    var edgeArray = Seq(Edge(0L, 0L, ""))

    // 添加点
    val pointSet = pointMap.keySet()
    // 遍历迭代map
    val point_iter = pointSet.iterator
    while (point_iter.hasNext) {
      val key = point_iter.next
      vertexArray = vertexArray :+ (pointMap.get(key), (key.split("_")(0), key.split("_")(1)))
    }

//    vertexArray.foreach(println(_))

    // 添加边
    val edgeSet = edgeMap.keySet()
    // 遍历迭代map
    val edge_iter = edgeSet.iterator
    while (edge_iter.hasNext) {
      val key = edge_iter.next
      edgeArray = edgeArray :+ Edge(key._1, key._2, edgeMap.get(key))
    }

//    edgeArray.foreach(println(_))

    // 构造vertexRDD和edgeRDD
    val vertexRDD: RDD[(Long, (String, String))] = spark.sparkContext.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[String]] = spark.sparkContext.parallelize(edgeArray)

    // 构造图Graph[VD,ED]
    val graph: Graph[(String, String), String] = Graph(vertexRDD, edgeRDD)

    


  }
}
