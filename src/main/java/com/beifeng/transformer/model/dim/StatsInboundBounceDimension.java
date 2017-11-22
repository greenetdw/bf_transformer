package com.beifeng.transformer.model.dim;

import com.beifeng.transformer.model.dim.base.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zhouning on 2017/11/22.
 * desc:统计Inbound的跳出会话维度类
 */
public class StatsInboundBounceDimension extends StatsDimension {

    private StatsCommonDimension statsCommon = new StatsCommonDimension();
    private String sid;
    private long serverTime;

    public StatsInboundBounceDimension() {
    }

    public StatsInboundBounceDimension(StatsCommonDimension statsCommon, String sid, long serverTime) {
        this.statsCommon = statsCommon;
        this.sid = sid;
        this.serverTime = serverTime;
    }

    public static StatsInboundBounceDimension clone(StatsInboundBounceDimension dimension) {
        return new StatsInboundBounceDimension(StatsCommonDimension.clone(dimension.statsCommon), dimension.sid, dimension.serverTime);
    }

    @Override
    public int compareTo(BaseDimension o) {
        StatsInboundBounceDimension other = (StatsInboundBounceDimension) o;
        int tmp = this.statsCommon.compareTo(other.statsCommon);
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.sid.compareTo(other.sid);
        if (tmp != 0) {
            return tmp;
        }
        tmp = Long.compare(this.serverTime, other.serverTime);
        return tmp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.statsCommon.write(dataOutput);
        dataOutput.writeUTF(this.sid);
        dataOutput.writeLong(this.serverTime);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.statsCommon.readFields(dataInput);
        this.sid = dataInput.readUTF();
        this.serverTime = dataInput.readLong();
    }

    public StatsCommonDimension getStatsCommon() {
        return statsCommon;
    }

    public void setStatsCommon(StatsCommonDimension statsCommon) {
        this.statsCommon = statsCommon;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public long getServerTime() {
        return serverTime;
    }

    public void setServerTime(long serverTime) {
        this.serverTime = serverTime;
    }
}
