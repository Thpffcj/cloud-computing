package cn.edu.nju

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by thpffcj on 2019/10/3.
 */
object GraphExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("GraphTest").setMaster("local")
    val sc = new SparkContext(conf)

    // 构建顶点 返回的这个Long其实是VertexId类型，都是一样的
    val users: RDD[(Long, (String, String))] =
      sc.parallelize(
        Array((3L, ("rxin", "student")),
        (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")),
        (2L, ("istoica", "prof"))))

    // 构建边 （边有个独特的类Edge，某种程度讲代表的就是一些关系）
    val relationships: RDD[Edge[String]] =
      sc.parallelize(
        Array(Edge(3L, 7L, "collab"),
          Edge(5L, 3L, "advisor"),
          Edge(2L, 5L, "colleague"),
          Edge(5L, 7L, "pi")))

    // 顶点和边，这样就构建了我们的图
    val graph = Graph(users, relationships)

    // .vertices获取到这个图中所有的顶点
    val count = graph.vertices.filter {
      case (id, (name, pos)) => {
        // 计算我们这个图中有多少个postdoc博士后
        pos == "postdoc"
      }
    }.count()

    // 1
    println(count)

    //.edges获取到这个图中所有的边，过滤出 源ID<目标ID 的数量
    val count1 = graph.edges.filter(e => e.srcId < e.dstId).count()

    // 3
    println(count1)

    sc.stop()
  }
}
