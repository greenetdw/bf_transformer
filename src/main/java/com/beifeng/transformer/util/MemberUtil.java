package com.beifeng.transformer.util;


import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

public class MemberUtil {

    private static Map<String, Boolean> cache = new LinkedHashMap<String, Boolean>() {
        private static final long serialVersionUID = -1489260427158655852L;

        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return this.size() > 10000;//最多保存1w个数据
        }
    };

    /**
     * 删除指定日期的数据
     *
     * @param date
     * @param connection
     * @throws SQLException
     */
    public static void deleteMemberInfoByDate(String date, Connection connection) throws SQLException {
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("DELETE FROM `member_info` WHERE `created`=?");
            pstmt.setString(1, date);
            pstmt.execute();
        } finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    //nothing
                }
            }
        }
    }

    /**
     * 判断member id 的格式是否正常，如果不正常，直接返回false，否则返回true
     *
     * @param memberId
     * @return
     */
    public static boolean isValidateMemberId(String memberId) {
        if (StringUtils.isNotBlank(memberId)) {
            return memberId.trim().matches("[0-9a-zA-Z]{1,32}");
        }
        return false;
    }


    /**
     * 判断memberId是否是一个新的会员，如果是，则返回true，否则返回false
     *
     * @param memberId   需要判断的memberId
     * @param connection 数据库连接信息
     * @return
     * @throws SQLException
     */
    public static boolean isNewMemberId(String memberId, Connection connection) throws SQLException {

        Boolean isNewMemberId = null;
        if (StringUtils.isNotBlank(memberId)) {
            //要求memberId不为空
            isNewMemberId = cache.get(memberId);
            if (isNewMemberId != null) {
                //表示该memberId没有进行数据库查询
                PreparedStatement pstmt = null;
                ResultSet rs = null;
                try {
                    pstmt = connection.prepareStatement("SELECT `member_id`,`last_visit_date` FROM `member_info` WHERE `member_id`=?");
                    pstmt.setString(1, memberId);
                    rs = pstmt.executeQuery();
                    if (rs.next()) {
                        //表示数据库中有对应的memberId，那么该memberId不是新的会员
                        isNewMemberId = false;
                    } else {
                        //表示数据库中没有对应的memberId，那么该memberId是新的会员id
                        isNewMemberId = true;
                    }
                    cache.put(memberId, isNewMemberId);
                } finally {
                    if (rs != null) {
                        try {
                            rs.close();
                        } catch (SQLException e) {
                            //nothing
                        }
                    }
                    if (pstmt != null) {
                        try {
                            pstmt.close();
                        } catch (SQLException e) {
                            //nothing
                        }
                    }
                }
            }
        }
        return isNewMemberId == null ? false : isNewMemberId.booleanValue();

    }

}
