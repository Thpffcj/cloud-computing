package cn.edu.nju.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by thpffcj on 2019/10/28.
 */
public class Test {

    public static void main(String[] args) {

        try {
            int[] time = new int[]{1483228800
                    ,1485907200
                    ,1488326400
                    ,1491004800
                    ,1493596800
                    ,1496275200
                    ,1498867200
                    ,1501545600
                    ,1504224000
                    ,1506816000
                    ,1509494400
                    ,1512086400
                    ,1514764800
                    ,1517443200
                    ,1519862400
                    ,1522540800
                    ,1525132800
                    ,1527811200
                    ,1530403200
                    ,1533081600
                    ,1535760000
                    ,1538352000
                    ,1541030400
                    ,1543622400
                    ,1546300800
                    ,1548979200
                    ,1551398400
                    ,1554076800
                    ,1556668800
                    ,1559347200
                    ,1561939200
                    ,1564617600
                    ,1567296000
                    ,1569888000};
            //调用Class.forName()方法加载驱动程序
            Class.forName("com.mysql.jdbc.Driver");
            System.out.println("成功加载MySQL驱动！");

            String url = "jdbc:mysql://172.19.240.128:3306/steam";    //JDBC的URL
            Connection conn;

            conn = DriverManager.getConnection(url, "root", "root");

            Statement stmt = conn.createStatement();
            System.out.println("成功连接到数据库！");

            String sql = "select distinct name from roll_up";
            ResultSet rs = stmt.executeQuery(sql);
            List<String> gameName = new ArrayList<>();
            while (rs.next()) {
                gameName.add(rs.getString(1));
            }

           for (int i = 1; i < time.length; i++) {
              for (int j = 0; j < gameName.size(); j++) {

                  sql = "select recommendations_up from roll_up where name = '" + gameName.get(j) + "' and time = " + time[i];
                  rs = stmt.executeQuery(sql);
                  int up1 = 0;
                  while (rs.next()) {
                      up1  = rs.getInt(1);
                  }

                  sql = "select recommendations_up from roll_up where name = '" + gameName.get(j) + "' and time = " + time[i - 1];
                  rs = stmt.executeQuery(sql);
                  int up2 = 0;
                  while (rs.next()) {
                      up2  = rs.getInt(1);
                  }

                  System.out.println(up1 + " " + up2);
                  int up = up1 + up2;
                  sql = "update roll_up set recommendations_up = " + up + " where name = '" + gameName.get(j) + "' and time = " + time[i];
                  System.out.println(sql);
                  stmt.executeUpdate(sql);
              }
           }

            rs.close();
            stmt.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
