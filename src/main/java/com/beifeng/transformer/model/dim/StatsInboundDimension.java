package com.beifeng.transformer.model.dim;

import com.beifeng.transformer.model.dim.base.BaseDimension;
import com.beifeng.transformer.model.dim.base.InboundDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zhouning on 2017/11/22.
 * desc: 统计inbound相关信息 维度类
 */
public class StatsInboundDimension extends StatsDimension {
    private StatsCommonDimension statsCommon = new StatsCommonDimension();
    private InboundDimension inbound = new InboundDimension();
    @Override
    public int compareTo(BaseDimension o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
