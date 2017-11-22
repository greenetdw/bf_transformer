package com.beifeng.util;

import com.beifeng.common.GlobalConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.sql.*;

/**
 * jdbc管理
 *
 * @author gerry
 */
public class JdbcManager {
    private static final Logger logger = Logger.getLogger(JdbcManager.class);

    /**
     * 根据配置获取获取关系型数据库的jdbc连接
     *
     * @param conf hadoop配置信息
     * @param flag 区分不同数据源的标志位
     * @return
     * @throws SQLException
     */
    public static Connection getConnection(Configuration conf, String flag) throws SQLException {
        String driverStr = String.format(GlobalConstants.JDBC_DRIVER, flag);
        String urlStr = String.format(GlobalConstants.JDBC_URL, flag);
        String usernameStr = String.format(GlobalConstants.JDBC_USERNAME, flag);
        String passwordStr = String.format(GlobalConstants.JDBC_PASSWORD, flag);

        String driverClass = conf.get(driverStr);
        String url = conf.get(urlStr);
        String username = conf.get(usernameStr);
        String password = conf.get(passwordStr);
        try {
            logger.info("driverClass: " + driverClass);
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            // nothing
        }
        return DriverManager.getConnection(url, username, password);
    }

    /**
     * 关闭数据库连接
     *
     * @param conn
     * @param tsmt
     * @param rs
     */
    public static void close(Connection conn, Statement tsmt, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                //nothing
            }
        }
        if (tsmt != null) {
            try {
                tsmt.close();
            } catch (SQLException e) {
                //nothing
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                //nothing
            }
        }
    }

}
