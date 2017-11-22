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


    public StatsInboundDimension() {
    }

    public StatsInboundDimension(StatsCommonDimension statsCommon, InboundDimension inbound) {
        this.statsCommon = statsCommon;
        this.inbound = inbound;
    }

    /**
     * 根据已有的实例对象克隆一个对象
     *
     * @param dimension
     * @return
     */
    public static StatsInboundDimension clone(StatsInboundDimension dimension) {
        return new StatsInboundDimension(StatsCommonDimension.clone(dimension.statsCommon), new InboundDimension(dimension.inbound));
    }

    @Override
    public int compareTo(BaseDimension o) {
        StatsInboundDimension other = (StatsInboundDimension) o;
        int tmp = this.statsCommon.compareTo(other.statsCommon);
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.inbound.compareTo(other.inbound);
        return tmp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.statsCommon.write(dataOutput);
        this.inbound.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.statsCommon.readFields(dataInput);
        this.inbound.readFields(dataInput);
    }

    public StatsCommonDimension getStatsCommon() {
        return statsCommon;
    }

    public void setStatsCommon(StatsCommonDimension statsCommon) {
        this.statsCommon = statsCommon;
    }

    public InboundDimension getInbound() {
        return inbound;
    }

    public void setInbound(InboundDimension inbound) {
        this.inbound = inbound;
    }
}
