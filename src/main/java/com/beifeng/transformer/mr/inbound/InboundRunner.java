package com.beifeng.transformer.mr.inbound;

import com.beifeng.common.EventLogConstants;
import com.beifeng.transformer.model.dim.StatsInboundDimension;
import com.beifeng.transformer.model.value.map.TextsOutputValue;
import com.beifeng.transformer.model.value.reduce.InboundReduceValue;
import com.beifeng.transformer.mr.TransformerBaseRunner;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * Created by zhouning on 2017/11/22.
 * desc:计算活跃用户和总会话的入口类
 */
public class InboundRunner extends TransformerBaseRunner {

    private static final Logger logger = Logger.getLogger(InboundRunner.class);

    public static void main(String[] args) {

        InboundRunner runner = new InboundRunner();
        runner.setupRunner("inbound", InboundRunner.class, InboundMapper.class, InboundReducer.class, StatsInboundDimension.class, TextsOutputValue.class, StatsInboundDimension.class, InboundReduceValue.class);

        try {
            runner.startRunner(args);
        } catch (Exception e) {
            logger.error("执行异常", e);
            throw new RuntimeException("执行异常", e);
        }

    }

    @Override
    protected Filter fetchHbaseFilter() {
        FilterList filterList = new FilterList();
        String[] columns = new String[]{
                EventLogConstants.LOG_COLUMN_NAME_REFERRER_URL,//前一个页面的url
                EventLogConstants.LOG_COLUMN_NAME_UUID,//uuid
                EventLogConstants.LOG_COLUMN_NAME_SESSION_ID,//会话id
                EventLogConstants.LOG_COLUMN_NAME_PLATFORM,//平台名称
                EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME,//服务器时间
                EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME//事件名称
        };
        filterList.addFilter(this.getColumnFilter(columns));
        filterList.addFilter(new SingleColumnValueFilter(InboundMapper.family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(EventLogConstants.EventEnum.PAGEVIEW.alias)));
        return filterList;
    }
}
