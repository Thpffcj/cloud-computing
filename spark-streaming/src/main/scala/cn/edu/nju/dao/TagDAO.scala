package cn.edu.nju.dao

import cn.edu.nju.domain.Tag
import java.sql.{Connection, PreparedStatement}
import cn.edu.nju.utils.MySQLUtils
import scala.collection.mutable.ListBuffer

/**
 * Created by thpffcj on 2019/10/25.
 */
object TagDAO {

  /**
   * 批量保存Tag到数据库
   */
  def insertTag(list: ListBuffer[Tag]): Unit = {

    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySQLUtils.getConnection()

      connection.setAutoCommit(false) //设置手动提交

      val sql = "insert into tag(game_name, number) values (?,?)"
      pstmt = connection.prepareStatement(sql)

      for (element <- list) {
        pstmt.setString(1, element.tagName)
        pstmt.setInt(2, element.number)

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
