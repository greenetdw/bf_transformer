package com.beifeng.transformer.mr.inbound;

import com.beifeng.common.KpiType;
import com.beifeng.transformer.model.dim.StatsInboundDimension;
import com.beifeng.transformer.model.value.map.TextsOutputValue;
import com.beifeng.transformer.model.value.reduce.InboundReduceValue;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by zhouning on 2017/11/22.
 * desc: 计算Reducer类
 */
public class InboundReducer extends Reducer<StatsInboundDimension, TextsOutputValue, StatsInboundDimension, InboundReduceValue> {
    private Set<String> uvs = new HashSet<>();
    private Set<String> visits = new HashSet<>();
    private InboundReduceValue outputValue = new InboundReduceValue();

    @Override
    protected void reduce(StatsInboundDimension key, Iterable<TextsOutputValue> values, Context context) throws IOException, InterruptedException {
        try {
            for (TextsOutputValue value : values) {
                this.uvs.add(value.getUuid());
                this.visits.add(value.getSid());
            }
            this.outputValue.setKpi(KpiType.valueOfName(key.getStatsCommon().getKpi().getKpiName()));
            this.outputValue.setUvs(this.uvs.size());
            this.outputValue.setVisit(this.visits.size());
            context.write(key, this.outputValue);

        } finally {
            //清空操作
            this.uvs.clear();
            this.visits.clear();
        }
    }
}
