package com.beifeng.transformer.model.value.map;

import com.beifeng.common.KpiType;
import com.beifeng.transformer.model.value.BaseStatsValueWritable;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zhouning on 2017/11/21.
 * desc: 定义一系列的字符串输出类
 */
public class TextsOutputValue extends BaseStatsValueWritable {
    private KpiType kpiType;
    private String uuid;//用户唯一标识符
    private String sid;//会话id

    public TextsOutputValue() {
    }

    public TextsOutputValue(String uuid, String sid) {
        this.uuid = uuid;
        this.sid = sid;
    }

    @Override
    public KpiType getKpi() {
        return this.kpiType;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.internalWriteString(out, this.uuid);
        this.internalWriteString(out, this.sid);
    }

    private void internalWriteString(DataOutput out, String value) throws IOException {
        if (StringUtils.isBlank(value)) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(value);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.uuid = this.internalReadString(in);
        this.sid = this.internalReadString(in);
    }

    private String internalReadString(DataInput in) throws IOException {
        return in.readBoolean() ? in.readUTF() : null;
    }

    public KpiType getKpiType() {
        return kpiType;
    }

    public void setKpiType(KpiType kpiType) {
        this.kpiType = kpiType;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }
}
