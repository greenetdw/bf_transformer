package com.beifeng.transformer.mr.locations;

import com.beifeng.common.EventLogConstants;
import com.beifeng.transformer.model.dim.StatsLocationDimension;
import com.beifeng.transformer.model.value.map.TextsOutputValue;
import com.beifeng.transformer.model.value.reduce.LocationReducerOutputValue;
import com.beifeng.transformer.mr.TransformerBaseRunner;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * Created by zhouning on 2017/11/21.
 * desc:
 */
public class LocationRunner extends TransformerBaseRunner {

    private static final Logger logger = Logger.getLogger(LocationRunner.class);

    public static void main(String[] args) {
        LocationRunner runner = new LocationRunner();
        runner.setupRunner("location", LocationRunner.class, LocationMapper.class, LocationReducer.class, StatsLocationDimension.class, TextsOutputValue.class, StatsLocationDimension.class, LocationReducerOutputValue.class);
        try {
            runner.startRunner(args);
        } catch (Exception e) {
            logger.error("执行异常", e);
            throw new RuntimeException("执行location维度统计出现异常", e);
        }
    }

    @Override
    protected Filter fetchHbaseFilter() {
        FilterList filterList = new FilterList();
        String[] columns = new String[]{
                EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME,//事件名称
                EventLogConstants.LOG_COLUMN_NAME_UUID,//用户id
                EventLogConstants.LOG_COLUMN_NAME_SESSION_ID,//会话id
                EventLogConstants.LOG_COLUMN_NAME_PLATFORM,//平台
                EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME,//服务器时间
                EventLogConstants.LOG_COLUMN_NAME_COUNTRY,//国家
                EventLogConstants.LOG_COLUMN_NAME_PROVINCE,//省份
                EventLogConstants.LOG_COLUMN_NAME_CITY//城市
        };
        filterList.addFilter(this.getColumnFilter(columns));
        //过滤只需要pageview事件
        filterList.addFilter(new SingleColumnValueFilter(LocationMapper.family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(EventLogConstants.EventEnum.PAGEVIEW.alias)));
        return filterList;
    }
}
