package com.beifeng.transformer.mr.inbound.bounce;

import com.beifeng.common.GlobalConstants;
import com.beifeng.transformer.model.dim.StatsInboundDimension;
import com.beifeng.transformer.model.dim.base.BaseDimension;
import com.beifeng.transformer.model.value.BaseStatsValueWritable;
import com.beifeng.transformer.model.value.reduce.InboundBounceReduceValue;
import com.beifeng.transformer.mr.IOutputCollector;
import com.beifeng.transformer.service.rpc.IDimensionConverter;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by zhouning on 2017/11/22.
 * desc:
 */
public class InboundBounceCollector implements IOutputCollector {
    @Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt, IDimensionConverter converter) throws SQLException, IOException {

        StatsInboundDimension inboundDimension = (StatsInboundDimension) key;
        InboundBounceReduceValue inboundBounceReduceValue = (InboundBounceReduceValue) value;

        int i = 0;
        pstmt.setInt(++i, converter.getDimensionIdByValue(inboundDimension.getStatsCommon().getPlatform()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(inboundDimension.getStatsCommon().getDate()));
        pstmt.setInt(++i, inboundDimension.getInbound().getId());//直接设置，在mapper类中已经设置
        pstmt.setInt(++i, inboundBounceReduceValue.getBounceNumber());
        pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        pstmt.setInt(++i, inboundBounceReduceValue.getBounceNumber());

        pstmt.addBatch();
    }
}
