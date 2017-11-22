package com.beifeng.transformer.model.value.reduce;

import com.beifeng.common.KpiType;
import com.beifeng.transformer.model.value.BaseStatsValueWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zhouning on 2017/11/22.
 * desc:
 */
public class InboundBounceReduceValue extends BaseStatsValueWritable {
    private KpiType kpi;
    private int bounceNumber;

    public InboundBounceReduceValue() {
    }

    public InboundBounceReduceValue(int bounceNumber) {
        this.bounceNumber = bounceNumber;
    }

    /**
     * 自增1
     */
    public void incrBounceNumber() {
        this.bounceNumber = this.bounceNumber + 1;
    }

    @Override
    public KpiType getKpi() {
        return this.kpi;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.bounceNumber);
        WritableUtils.writeEnum(dataOutput, this.kpi);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.bounceNumber = dataInput.readInt();
        this.kpi = WritableUtils.readEnum(dataInput, KpiType.class);
    }

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }

    public int getBounceNumber() {
        return bounceNumber;
    }

    public void setBounceNumber(int bounceNumber) {
        this.bounceNumber = bounceNumber;
    }
}
