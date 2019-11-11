package cn.edu.nju

import java.io.PrintWriter
import java.util
import java.util.concurrent.ConcurrentHashMap

import com.alibaba.fastjson.JSON
import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document

import scala.util.Random

/**
 * Created by thpffcj on 2019/11/2.
 * hours_3_10W 53 160
 * steam_3_10W 53 160
 * hours_5_20W 98 464
 * steam_5_20W 96 452
 * hours_7_30W 103 535
 * steam_7_30W 103 535
 * hours_6_30W 103 535
 * steam_6_30W 103 535
 *
 */
object GraphProcess {

  // 点集，根据用户id或游戏名找点id
  // user_76561198380840992 1L
  // game_CODE VEIN 2L
  val pointMap = new ConcurrentHashMap[String, Long]()

  // 评论边
  val edgeMap1 = new ConcurrentHashMap[(Long, Long), String]()

  // 时长边
  val edgeMap2 = new ConcurrentHashMap[(Long, Long), String]()

  //  点权重map，根据图中点id得到权重
  // 1L 10
  val weightMap = new ConcurrentHashMap[Long, Int]()

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[4]").setAppName("GraphProcess")
    conf.set("spark.mongodb.input.uri", "mongodb://localhost:27017/test.China.reviews_official_30W")
    conf.set("spark.mongodb.input.partitioner", "MongoPaginateBySizePartitioner")
    conf.set("spark.mongodb.output.uri", "mongodb://localhost:27017/test.steam.graph_vertice")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val frame: DataFrame = MongoSpark.load(spark)

    var key = 0L

    // 需要collect到一个节点上，key递增
    frame.collect().foreach(row => {

      val gameArray = row.getAs("game").toString.split(",")
      var game = gameArray(0)
      game = game.substring(1, game.length)
      // 过滤前端无法识别非法字符，比如表情等
      val gameNamePatterns = "[^\\u4e00-\\u9fa5a-zA-Z0-9 ]".r
      // 游戏名称
      game = gameNamePatterns.replaceAllIn(game, "")

      val jsonAuthor = JSON.parse(row.getAs("author").toString)
      val authorArray = jsonAuthor.toString.split(",")
      // 用户id
      val userId = authorArray(4)
      // 游玩时长
      val hours = authorArray(5)

      // 评论
      var review = row.getAs("review").toString
      val reviewPatterns = "[^\\u4e00-\\u9fa5a-zA-Z0-9 ]".r
      review = reviewPatterns.replaceAllIn(review, "")

      // 玩家顶点
      val playerKey = "user_" + userId
      var playerPoint = 0L
      if (pointMap.containsKey(playerKey)) {
        playerPoint = pointMap.get(playerKey)
        // 权重+1
        weightMap.put(playerPoint, weightMap.get(playerPoint) + 1)
      } else {
        // 点id递增
        this.synchronized {
          key = key + 1
          playerPoint = key
        }
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
        this.synchronized {
          key = key + 1
          gamePoint = key
        }
        pointMap.put(gameKey, gamePoint)
        // 权重赋予1
        weightMap.put(gamePoint, 1)
      }

//      println(gameKey + " " + gamePoint)


//      println("---------")
//      println(playerPoint)
//      println(gamePoint)

      edgeMap1.put((playerPoint, gamePoint), review)
      edgeMap2.put((playerPoint, gamePoint), hours)
    })

    println("foreach 结束")

//    val weightSet = weightMap.keySet()
//    val weight_iter = weightSet.iterator
//    while (weight_iter.hasNext) {
//      val key = weight_iter.next
//      if (weightMap.get(key) > 5000) {
//        println("----------")
//        println(key)
//        println(weightMap.get(key))
//        println("-----------")
//      }
//    }

    // 点集
    var vertexArray = Seq((0L, ("test", "test")))
    // 评论边
    var edgeArray1 = Seq(Edge(0L, 0L, ""))
    // 时长边
    var edgeArray2 = Seq(Edge(0L, 0L, ""))

    // 添加点
    val pointSet = pointMap.keySet()
    // TODO 遍历迭代map，这个阶段非常耗时，如何改进？
    val point_iter = pointSet.iterator
    while (point_iter.hasNext) {
      val key = point_iter.next
      val name = key.split("_")
      vertexArray = vertexArray :+ (pointMap.get(key), (name(0), name(1)))
    }

    println("遍历点集结束")

    // 添加评论边
    val edgeSet1 = edgeMap1.keySet()
    // 遍历迭代map
    val edge_iter1 = edgeSet1.iterator
    while (edge_iter1.hasNext) {
      val key = edge_iter1.next
      edgeArray1 = edgeArray1 :+ Edge(key._1, key._2, edgeMap1.get(key))
    }

    println("遍历评论边结束")

    // 添加时长边
    val edgeSet2 = edgeMap2.keySet()
    // 遍历迭代map
    val edge_iter2 = edgeSet2.iterator
    while (edge_iter2.hasNext) {
      val key = edge_iter2.next
      edgeArray2 = edgeArray2 :+ Edge(key._1, key._2, edgeMap2.get(key))
    }

    println("遍历结束")

    // 构造vertexRDD和edgeRDD
    val vertexRDD: RDD[(Long, (String, String))] = spark.sparkContext.parallelize(vertexArray)
    val edgeRDD1: RDD[Edge[String]] = spark.sparkContext.parallelize(edgeArray1)
    val edgeRDD2: RDD[Edge[String]] = spark.sparkContext.parallelize(edgeArray2)

    // 构造图Graph[VD,ED]
    var contentGraph: Graph[(String, String), String] = Graph(vertexRDD, edgeRDD1)

    println("构造contentGraph结束")

    // 构建子图，过滤评论为空的边
    contentGraph = contentGraph.subgraph(epred = e => !e.attr.equals(""))
    // 构建子图，过滤游戏权重大于10的
    contentGraph = contentGraph.subgraph(vpred = (id, vd) => {
      ((vd._1.equals("game") & weightMap.get(id) > 10) | (vd._1.equals("user")))
    })

    val degreeThreshold = 6
    // 度数>degreeThreshold的点集
    val contentDegreeArray = contentGraph.degrees.filter(_._2 > degreeThreshold).map(_._1).collect()

    // 保留度数符合规定的点
    contentGraph = contentGraph.subgraph(vpred = (id, vd) => {
      contentDegreeArray.contains(id)
    })

    // 边的转换操作，去除前端无法识别的字符，如评论表情等
    val reviewPatterns = "[^\\u4e00-\\u9fa5a-zA-Z0-9 ]".r
    contentGraph.mapEdges(e => e.attr = reviewPatterns.replaceAllIn(e.attr, ""))

    println("处理contentGraph结束")

    // 时长图
    var hourGraph: Graph[(String, String), String] = Graph(vertexRDD, edgeRDD2)

    println("构造hourGraph结束")

    // TODO 顶点的转换操作，根据用户id寻找用户名称
    hourGraph = hourGraph.mapVertices {
      case (id, (types, name)) => (types, name)
    }

    hourGraph = hourGraph.subgraph(vpred = (id, vd) => {
      ((vd._1.equals("game") & weightMap.get(id) > 10) | (vd._1.equals("user")))
    })

    // 度数>0的点集
    val hourDegreeArray = hourGraph.degrees.filter(_._2 > degreeThreshold).map(_._1).collect()

    // 去除孤立的点
    hourGraph = hourGraph.subgraph(vpred = (id, vd) => {
      hourDegreeArray.contains(id)
    })

    println("处理hourGraph结束")

    // 独立群体检测
    hourGraph.connectedComponents
      .vertices
      .map(_.swap)
      .groupByKey()
      .map(_._2)
      .foreach(println)

    /**
     * 将点数据写入MongoDB
     * Spark的算子是在executor上执行的，数据也是放在executor上。executor和driver并不在同一个jvm（local[*]除外），
     * 所以算子是不能访问在driver上的SparkSession对象
     * 如果一定要“在算子里访问SparkSession”，那你只能把数据collect回Driver，然后用Scala 集合的算子去做。这种情况下只能适
     * 用于数据量不大（多大取决于你分配给Driver的内存）
     */
    hourGraph.vertices.collect.foreach(v => {

      val id = v._1.toString
      val name = v._2.toString

      writeVerticesToMongodb(spark, id, name)
    })


    // 输出到文件
    val outputPath = "src/main/resources/"
    val pw1 = new PrintWriter(outputPath + "steam/hours_6_30W.gexf")
    pw1.write(hoursToGexf(hourGraph))
    pw1.close()

    val pw2 = new PrintWriter(outputPath + "steam/steam_6_30W.gexf")
    pw2.write(gameToGexf(contentGraph))
    pw2.close()

    spark.close()
  }

  /**
   * 点数据写入MongoDB
   */
  def writeVerticesToMongodb(spark: SparkSession, id: String, name: String) = {

    val document = new Document()
    document.append("verticeId", id).append("name", name)

    val seq = Seq(document)
    val df = spark.sparkContext.parallelize(seq)

    // 将数据写入mongo
    MongoSpark.save(df)
  }

  /**
   * 边据写入MongoDB
   */
  def writeEdgesToMongodb(spark: SparkSession, srcId: String, dstId: String, attr: String) = {

    val document = new Document()
    document.append("srcId", srcId).append("dstId", dstId).append("attr", attr)

    val seq = Seq(document)
    val df = spark.sparkContext.parallelize(seq)

    // 将数据写入mongo
    MongoSpark.save(df)
  }

  /**
   * 用户-游戏图，相比底下的图需要指定x,y的坐标
   *
   * @param graph
   * @tparam VD
   * @tparam ED
   * @return
   */
  def gameToGexf[VD, ED](graph: Graph[VD, ED]) = {

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
        "<node id=\"" + v._1 + "\" label=\"" + name + "\">\n" +
          "<attvalues>\n" +
          "<attvalue for=\"modularity_class\" value=\"" + attvalue + "\"></attvalue>\n" +
          "</attvalues>\n" +
          "<viz:size value=\"" + weightMap.get(v._1) + "\"></viz:size>\n" +
          // (x, y) 坐标
          "<viz:position x=\"" + (Random.nextInt(20000) - 10000).toString + "\" y=\"" + (Random.nextInt(20000) - 10000).toString + "\" z=\"0.0\"></viz:position>\n" +
          "<viz:color " + color + "></viz:color>\n" +
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
   * 时间输出为指定gexf格式
   *
   * @param graph ：图
   * @tparam VD
   * @tparam ED
   * @return
   */
  def hoursToGexf[VD, ED](graph: Graph[VD, ED]) = {

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
        "<node id=\"" + v._1 + "\" label=\"" + name + "\">\n" +
          "<attvalues>\n" +
          "<attvalue for=\"modularity_class\" value=\"" + attvalue + "\"></attvalue>\n" +
          "</attvalues>\n" +
          "<viz:size value=\"" + weightMap.get(v._1) + "\"></viz:size>\n" +
          "<viz:color " + color + "></viz:color>\n" +
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
}
