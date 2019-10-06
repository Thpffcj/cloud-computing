package cn.edu.nju

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by thpffcj on 2019/10/3.
 *
 * 图计算官网案例示范
 * 主要解决项目中遇到的 把同一个用户识别出来，如果是同一个用户就合并到一起
 */
object ConnectedComponentsExample {

  def main(args: Array[String]): Unit = {

    // graphx 基于RDD
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("ConnectedComponentsExample")
    val sc = new SparkContext(conf)

    // 构建出来图有多种方式
    val graph = GraphLoader.edgeListFile(sc, "src/test/resources/follows.txt")
    graph.vertices.foreach(println(_))

    /** 就是把所有的数字作为key，value都写为1
     * (4,1)
     * (1,1)
     * (6,1)
     * (3,1)
     * (7,1)
     * (5,1)
     * (2,1)
     */
    /**
     * .connectedComponents()计算每个顶点的连接组件成员，并返回带有顶点的图形
     * 包含该顶点的连通组件中包含最低顶点id的值。
     */
    val cc = graph.connectedComponents().vertices
    cc.foreach(println(_))
    /**
     * value是这一组中最小的数，是好友的value是一样的
     * (4,4)
     * (6,4)
     * (7,4)
     *
     * (1,1)
     * (3,1)
     * (5,1)
     * (2,1)
     *
     */
    val users = sc.textFile("src/test/resources/user.txt").map(line => {
      // 因为要join，所以要变成kv形式
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    })

    // (1,Thpffcj1)  join  (1,1)
    // (1,(Thpffcj1,1) 代表的是同一个好友的那个id
    users.join(cc).map {
      case (id, (username, cclastid)) => (cclastid, username)
    }.reduceByKey((x: String, y: String) => x + "," + y)
      .foreach(tuple => {
        println(tuple._2)
      })

    sc.stop()
  }
}
