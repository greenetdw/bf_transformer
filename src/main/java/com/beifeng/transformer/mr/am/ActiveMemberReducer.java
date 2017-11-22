package com.beifeng.transformer.mr.am;

import com.beifeng.common.KpiType;
import com.beifeng.transformer.model.dim.StatsUserDimension;
import com.beifeng.transformer.model.value.map.TimeOutputValue;
import com.beifeng.transformer.model.value.reduce.MapWritableValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ActiveMemberReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, MapWritableValue> {

    private Set<String> unique = new HashSet<>();
    private MapWritableValue outputValue = new MapWritableValue();
    private MapWritable map = new MapWritable();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        try {
            //将memberId 添加到set集合中，方便进行统计memberId的去重个数
            for (TimeOutputValue value : values) {
                this.unique.add(value.getId());
            }

            //设置value
            this.map.put(new IntWritable(-1), new IntWritable(this.unique.size()));
            this.outputValue.setValue(map);
            //设置Kpi
            this.outputValue.setKpi(KpiType.valueOfName(key.getStatsCommon().getKpi().getKpiName()));

            //进行输出
            context.write(key, this.outputValue);

        } finally {
            //清空操作
            this.unique.clear();
        }
    }
}
