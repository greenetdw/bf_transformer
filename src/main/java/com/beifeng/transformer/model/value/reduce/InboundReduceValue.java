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
public class InboundReduceValue extends BaseStatsValueWritable {
    private KpiType kpi;
    private int uvs;
    private int visit;

    @Override
    public KpiType getKpi() {
        return this.kpi;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.uvs);
        dataOutput.writeInt(this.visit);
        WritableUtils.writeEnum(dataOutput, this.kpi);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.uvs = dataInput.readInt();
        this.visit = dataInput.readInt();
        this.kpi = WritableUtils.readEnum(dataInput, KpiType.class);
    }

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }

    public int getUvs() {
        return uvs;
    }

    public void setUvs(int uvs) {
        this.uvs = uvs;
    }

    public int getVisit() {
        return visit;
    }

    public void setVisit(int visit) {
        this.visit = visit;
    }
}
