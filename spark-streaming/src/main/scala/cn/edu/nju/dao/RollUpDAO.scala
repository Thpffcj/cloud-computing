package cn.edu.nju.dao

import cn.edu.nju.domain.RollUp
import java.sql.{Connection, PreparedStatement}

import cn.edu.nju.utils.MySQLUtils

import scala.collection.mutable.ListBuffer

/**
 * Created by thpffcj on 2019/10/25.
 */
object RollUpDAO {

  /**
   * 批量保存RollUp到数据库
   */
  def insertRollUp(list: ListBuffer[(String, Int, Int, Int)]): Unit = {

    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySQLUtils.getConnection()

      connection.setAutoCommit(false) //设置手动提交

      val sql = "insert into roll_up(name, time, recommendations_up, recommendations_down) values (?,?,?,?) "
      pstmt = connection.prepareStatement(sql)

      for (element <- list) {
        pstmt.setString(1, element._1)
        pstmt.setInt(2, element._2)
        pstmt.setInt(3, element._3)
        pstmt.setInt(4, element._4)

        pstmt.addBatch()
      }

      pstmt.executeBatch() // 执行批量处理
      connection.commit() // 手工提交
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(connection, pstmt)
    }
  }
}
