package cn.edu.nju.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * Created by thpffcj on 2019/10/25.
 */
object MySQLUtils {

  /**
   * 获取MySQL的连接
   */
  def getConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/steam?useUnicode=true&characterEncoding=utf-8", "root", "000000")
  }

  /**
   * 释放数据库连接等资源
   * @param connection
   * @param pstmt
   */
  def release(connection: Connection, pstmt: PreparedStatement): Unit = {
    try {
      if (pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }

  def main(args: Array[String]) {
    println(getConnection())
  }
}
