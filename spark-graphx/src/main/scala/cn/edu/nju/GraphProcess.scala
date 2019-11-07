package cn.edu.nju

import java.io.PrintWriter
import java.util

import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document

/**
 * Created by thpffcj on 2019/11/2.
 */
object GraphProcess {

  val pointMap = new util.HashMap[String, Long]()
  val edgeMap = new util.HashMap[(Long, Long), String]()
  //  权重map，根据id得到权重
  val weightMap = new util.HashMap[Long, Int]()

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[4]").setAppName("GraphProcess")
    conf.set("spark.mongodb.input.uri", "mongodb://localhost:27017/test.China.reviews")
    conf.set("spark.mongodb.input.partitioner", "MongoPaginateBySizePartitioner")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val frame: DataFrame = MongoSpark.load(spark)

    var key = 0L

    frame.foreach(row => {

      // 用户信息
      val jsonPlayer = row.getAs("user").toString.split(",")
      val player = jsonPlayer(0).substring(1, jsonPlayer(0).length - 2)

      // 游戏信息
      val jsonGame = row.getAs("game").toString.split(",")
      val game = jsonGame(0).substring(1)

      // 评论
      val content = row.getAs("content").toString.replace("<br>", "")

      // 游玩时长
      val patterns = "[\\u4e00-\\u9fa5]".r
      val hours = patterns.replaceAllIn(row.getAs("hours").toString, "")

      println(hours)

      // 玩家顶点
      val playerKey = "user_" + player
      var playerPoint = 0L
      if (pointMap.containsKey(playerKey)) {
        playerPoint = pointMap.get(playerKey)
        // 权重+1
        weightMap.put(playerPoint, weightMap.get(playerPoint) + 1)
      } else {
        key = key + 1
        playerPoint = key
        pointMap.put(playerKey, playerPoint)
        // 权重赋予1
        weightMap.put(playerPoint, 1)
      }

      // 游戏顶点
      val gameKey = "game_" + game
      var gamePoint = 0L
      if (pointMap.containsKey(gameKey)) {
        gamePoint = pointMap.get(gameKey)
        // 权重+1
        weightMap.put(gamePoint, weightMap.get(gamePoint) + 1)
      } else {
        key = key + 1
        gamePoint = key
        pointMap.put(gameKey, gamePoint)
        // 权重赋予1
        weightMap.put(gamePoint, 1)
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

    // 添加边
    val edgeSet = edgeMap.keySet()
    // 遍历迭代map
    val edge_iter = edgeSet.iterator
    while (edge_iter.hasNext) {
      val key = edge_iter.next
      edgeArray = edgeArray :+ Edge(key._1, key._2, edgeMap.get(key))
    }

    // 构造vertexRDD和edgeRDD
    val vertexRDD: RDD[(Long, (String, String))] = spark.sparkContext.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[String]] = spark.sparkContext.parallelize(edgeArray)

    // 构造图Graph[VD,ED]
    val graph: Graph[(String, String), String] = Graph(vertexRDD, edgeRDD)

    graph.vertices.foreach(println(_))

    // 输出到文件
    val outputPath = "src/main/resources/"
    val pw1 = new PrintWriter(outputPath + "steam.xml")
    pw1.write(toGexf(graph))

    pw1.close()

    spark.close()
  }

  /**
   * 输出为指定gexf格式
   *
   * @param graph ：图
   * @tparam VD
   * @tparam ED
   * @return
   */
  def toGexf[VD, ED](graph: Graph[VD, ED]) = {

    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\" xmlns:viz=\"http://www.gexf.net/1.2draft/viz\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.gexf.net/1.2draft http://www.gexf.net/1.2draft/gexf.xsd\">\n" +
      "<graph defaultedgetype=\"directed\" mode=\"static\">\n" +
      "<attributes class=\"node\" mode=\"static\">\n" +
      "<attribute id=\"modularity_class\" title=\"Modularity Class\" type=\"integer\"></attribute>\n" +
      "</attributes>\n" +
      "<nodes>\n " +
      graph.vertices.map(v => {
        // 根据类别填充颜色和attvalue
        val types = v._2.toString.split(",")(0).replace("(", "")
        val name = v._2.toString.split(",")(1).replace(")", "")
        var color = ""
        var attvalue = 0
        if (types.equals("user")) {
          color = "r=\"236\" g=\"81\" b=\"72\""
          attvalue = 1
        } else {
          color = "r=\"236\" g=\"181\" b=\"72\""
          attvalue = 0
        }
        "<node id=\"" + v._1 + "\" label=\"" + types + "-" + name + "\">\n" +
          "<attvalues>\n" +
          "<attvalue for=\"modularity_class\" value=\"" + attvalue + "\"></attvalue>\n" +
          "</attvalues>\n" +
          "<viz:size value=\"" + weightMap.get(v._1) + "\"></viz:size>\n" +
          "<viz:color " + color +"></viz:color>\n" +
          "</node>\n"
      }).collect().mkString +
      "</nodes>\n  " +
      "<edges>\n" +
      graph.edges.map(e => {
        "<edge source=\"" + e.srcId + "\" target=\"" + e.dstId + "\" label=\"" + e.attr + "\" weight=\"" + 1.0 + "\"/>\n"
      }).collect().mkString +
      "</edges>\n" +
      "</graph>\n" +
      "</gexf>"
  }

  /**
   * 数据写入MongoDB
   *
   */
  def writeToMongodb() = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoDBProcess")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.China.graph")
      .getOrCreate()

    // 设置log级别
    spark.sparkContext.setLogLevel("WARN")

    val document1 = new Document()
    document1.append("name", "sunshangxiang").append("age", 18).append("sex", "female")

    val seq = Seq(document1)
    val df = spark.sparkContext.parallelize(seq)

    // 将数据写入mongo
    MongoSpark.save(df)

    spark.stop()
  }
}
