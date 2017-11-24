package com.beifeng.transformer.mr.au;

import com.beifeng.common.GlobalConstants;
import com.beifeng.transformer.model.dim.StatsUserDimension;
import com.beifeng.transformer.model.dim.base.BaseDimension;
import com.beifeng.transformer.model.value.BaseStatsValueWritable;
import com.beifeng.transformer.model.value.reduce.MapWritableValue;
import com.beifeng.transformer.mr.IOutputCollector;
import com.beifeng.transformer.service.rpc.IDimensionConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by zhouning on 2017/11/20.
 * desc:
 */
public class HourlyActiveUserCollector implements IOutputCollector {
    @Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt, IDimensionConverter converter) throws SQLException, IOException {

        StatsUserDimension statUser = (StatsUserDimension) key;
        MapWritableValue mapWritableValue = (MapWritableValue) value;
        MapWritable map = mapWritableValue.getValue();

        //hourly_active_user
        int i = 0;
        pstmt.setInt(++i, converter.getDimensionIdByValue(statUser.getStatsCommon().getPlatform()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statUser.getStatsCommon().getDate()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statUser.getStatsCommon().getKpi()));

        //设置每小时的情况
        for (i++; i < 28; i++) {
            int v = ((IntWritable) map.get(new IntWritable(i - 4))).get();
            pstmt.setInt(i, v);
            pstmt.setInt(i + 25, v);
        }
        pstmt.setString(i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        pstmt.addBatch();

    }
}
