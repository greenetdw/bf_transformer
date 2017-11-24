package com.beifeng.transformer.mr.inbound;

import com.beifeng.common.GlobalConstants;
import com.beifeng.transformer.model.dim.StatsInboundDimension;
import com.beifeng.transformer.model.dim.base.BaseDimension;
import com.beifeng.transformer.model.value.BaseStatsValueWritable;
import com.beifeng.transformer.model.value.reduce.InboundReduceValue;
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
public class InboundCollector implements IOutputCollector {

    @Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt, IDimensionConverter converter) throws SQLException, IOException {

        StatsInboundDimension inboundKey = (StatsInboundDimension) key;
        InboundReduceValue inboundValue = (InboundReduceValue) value;

        int i = 0;
        pstmt.setInt(++i, converter.getDimensionIdByValue(inboundKey.getStatsCommon().getPlatform()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(inboundKey.getStatsCommon().getDate()));
        pstmt.setInt(++i, inboundKey.getInbound().getId());//直接设置，在mapper类中已经设置
        pstmt.setInt(++i, inboundValue.getUvs());
        pstmt.setInt(++i, inboundValue.getVisit());
        pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        pstmt.setInt(++i, inboundValue.getUvs());
        pstmt.setInt(++i, inboundValue.getVisit());
        pstmt.addBatch();
    }
}
