package com.beifeng.transformer.hive;

import com.beifeng.common.GlobalConstants;
import com.beifeng.util.JdbcManager;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by zhouning on 2017/11/26.
 * desc:获取order相关属性
 */
public class OrderInfoUDF extends UDF {

    private Connection conn = null;
    private Map<String, InnerOrderInfo> cache = new LinkedHashMap<String, InnerOrderInfo>() {
        private static final long serialVersionUID = -4323841499046880391L;

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, InnerOrderInfo> eldest) {
            return this.size() > 100;
        }
    };


    public OrderInfoUDF() {
        Configuration conf = new Configuration();
        conf.addResource("transformer-env.xml");
        try {
            this.conn = JdbcManager.getConnection(conf, GlobalConstants.WAREHOUSE_OF_REPORT);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("创建mysql连接异常", e);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    JdbcManager.close(conn, null, null);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }));
    }

    /**
     * 获取对应订单的订单金额，如果数据库中没有对应的订单，那么直接返回０
     *
     * @param orderId
     * @return
     */
    public int evaluate(String orderId) {
        if (StringUtils.isBlank(orderId)) {
            throw new IllegalArgumentException("参数异常，订单id不能为空");
        }
        orderId = orderId.trim();
        InnerOrderInfo info = this.fetchInnerOrderInfo(orderId);
        return info == null ? 0 : info.amount;
    }

    /**
     * 根据订单id和标识获取对应的订单值
     *
     * @param orderId
     * @param flag
     * @return
     */
    public String evaluate(String orderId, String flag) {
        if (StringUtils.isBlank(orderId)) {
            throw new IllegalArgumentException("参数异常，订单id不能为空");
        }
        orderId = orderId.trim();
        InnerOrderInfo info = this.fetchInnerOrderInfo(orderId);
        switch (flag) {
            case "pl":
                return info == null || StringUtils.isBlank(info.platform) ? GlobalConstants.DEFAULT_VALUE : info.platform;
            case "cut":
                return info == null || StringUtils.isBlank(info.currencyType) ? GlobalConstants.DEFAULT_VALUE : info.currencyType;
            case "pt":
                return info == null || StringUtils.isBlank(info.paymentType) ? GlobalConstants.DEFAULT_VALUE : info.paymentType;
            default:
                throw new IllegalArgumentException("参数异常，flag必须为(pl,cut,pt)中的一个，给定的是：" + flag);
        }
    }


    private InnerOrderInfo fetchInnerOrderInfo(String orderId) {
        InnerOrderInfo info = this.cache.get(orderId);
        if (info == null) {
            //进行查询操作
            PreparedStatement pstmt = null;
            ResultSet rs = null;
            try {
                pstmt = conn.prepareStatement("SELECT order_id,platform,s_time,currency_type,payment_type,amount FROM order_info WHERE order_id=?");
                pstmt.setString(1, orderId);
                rs = pstmt.executeQuery();
                if (rs.next()) {
                    info = new InnerOrderInfo();
                    info.orderId = orderId;
                    info.currencyType = rs.getString("currency_type");
                    info.paymentType = rs.getString("payment_type");
                    info.platform = rs.getString("platform");
                    info.sTime = rs.getLong("s_time");
                    info.amount = rs.getInt("amount");
                }
                this.cache.put(orderId, info);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("查询数据库异常");
            } finally {
                JdbcManager.close(null, pstmt, rs);
            }
        }
        return info;
    }


    /**
     * 内部类
     */
    private static class InnerOrderInfo {
        public String orderId;
        public String currencyType;
        public String paymentType;
        public String platform;
        public long sTime;
        public int amount;
    }


}
