package cn.edu.nju

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD

/**
 * Created by thpffcj on 2019/11/2.
 */
object GraphProcess {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("GraphProcess")
    val sc = new SparkContext(conf)

    // 设置顶点和边，注意顶点和边都是用元组定义的Array
    // 顶点的数据类型是VD:(String, Int)
    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )

    // 边的数据类型ED:Int
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )

    // 构造vertexRDD和edgeRDD
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

  }
}
