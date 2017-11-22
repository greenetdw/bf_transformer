package com.beifeng.transformer.model.value.reduce;

import com.beifeng.common.KpiType;
import com.beifeng.transformer.model.value.BaseStatsValueWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zhouning on 2017/11/21.
 * desc: 自定义Location统计reducer的输出value类
 */
public class LocationReducerOutputValue extends BaseStatsValueWritable {
    private KpiType kpiType;
    private int uvs;//活跃用户数
    private int visits;//会话个数
    private int bounceNumber;//跳出会话个数

    @Override
    public KpiType getKpi() {
        return this.kpiType;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.uvs);
        out.writeInt(this.visits);
        out.writeInt(this.bounceNumber);
        WritableUtils.writeEnum(out, this.kpiType);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.uvs = in.readInt();
        this.visits = in.readInt();
        this.bounceNumber = in.readInt();
        WritableUtils.readEnum(in, KpiType.class);
    }

    public KpiType getKpiType() {
        return kpiType;
    }

    public void setKpiType(KpiType kpiType) {
        this.kpiType = kpiType;
    }

    public int getUvs() {
        return uvs;
    }

    public void setUvs(int uvs) {
        this.uvs = uvs;
    }

    public int getVisits() {
        return visits;
    }

    public void setVisits(int visits) {
        this.visits = visits;
    }

    public int getBounceNumber() {
        return bounceNumber;
    }

    public void setBounceNumber(int bounceNumber) {
        this.bounceNumber = bounceNumber;
    }
}
