package cn.edu.nju.utils;

import cn.edu.nju.domain.TagObject;
import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

/**
 * Created by thpffcj on 2020/2/26.
 */
public class DbPool {

    private static DbPool instance;

    private ComboPooledDataSource ds;

    private DbPool() throws Exception {
        ds = new ComboPooledDataSource();
        ds.setDriverClass("oracle.jdbc.driver.OracleDriver");  //驱动
        ds.setJdbcUrl("jdbc:oracle:thin:@localhost:1521:orcl");  //地址
        ds.setUser("test0816");  //数据库用户名
        ds.setPassword("934617699");  //数据库用户密码

        // 初始化时获取三个连接，取值应在minPoolSize与maxPoolSize之间。Default: 5 initialPoolSize
        ds.setInitialPoolSize(5);
        // 连接池中保留的最大连接数。Default:  20 maxPoolSize
        ds.setMaxPoolSize(20);
        // 连接池中保留的最小连接数。
        ds.setMinPoolSize(1);
        // 当连接池中的连接耗尽的时候c3p0一次同时获取的连接数。Default: 5 acquireIncrement
        ds.setAcquireIncrement(10);
    }

    // 用来返回该对象
    public static final DbPool getInstance() {

        if (instance == null) {
            try {
                instance = new DbPool();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return instance;
    }

    // 返回一个连接
    public synchronized final Connection getConnection() {
        try {
            return ds.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        DbPool dbPool = DbPool.getInstance() ;

        List<TagObject> list = new ArrayList<>();

        Connection connection = dbPool.getConnection();
        String sql = "select * from person " ;

        try {
            PreparedStatement pt = connection.prepareStatement(sql) ;
            ResultSet rt = pt.executeQuery() ;

            while(rt.next()) {
                TagObject tag = new TagObject();
                tag.setLabel(rt.getString("label"));
                tag.setValue(rt.getInt("value"));
                list.add(tag) ;
            }

            for(TagObject tag : list) {
                System.out.println(tag);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
