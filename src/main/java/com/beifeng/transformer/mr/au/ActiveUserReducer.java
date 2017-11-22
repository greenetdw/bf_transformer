package com.beifeng.transformer.mr.au;

import com.beifeng.common.DateEnum;
import com.beifeng.common.KpiType;
import com.beifeng.transformer.model.dim.StatsUserDimension;
import com.beifeng.transformer.model.value.map.TimeOutputValue;
import com.beifeng.transformer.model.value.reduce.MapWritableValue;
import com.beifeng.util.TimeUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ActiveUserReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, MapWritableValue> {
    private Set<String> unique = new HashSet<>();
    private MapWritableValue outputValue = new MapWritableValue();
    private MapWritable map = new MapWritable();

    private Map<Integer, Set<String>> hourlyUnique = new HashMap<>();
    private MapWritable hourlyMap = new MapWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        //进行初始化操作
        for (int i = 0; i < 24; i++) {
            this.hourlyMap.put(new IntWritable(i), new IntWritable(0));
            this.hourlyUnique.put(i, new HashSet<String>());
        }
    }

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        try {
            String kpiName = key.getStatsCommon().getKpi().getKpiName();
            if (KpiType.HOURLY_ACTIVE_USER.name.equals(kpiName)) {
                //计算hourly active user
                for (TimeOutputValue value : values) {
                    //计算出访问的小时，从[0,23]的区间段
                    int hour = TimeUtil.getDateInfo(value.getTime(), DateEnum.HOUR);
                    this.hourlyUnique.get(hour).add(value.getId());//将会话id添加到对应的时间段中
                }

                //设置kpi
                this.outputValue.setKpi(KpiType.HOURLY_ACTIVE_USER);
                //设置value
                for (Map.Entry<Integer, Set<String>> entry : this.hourlyUnique.entrySet()) {
                    this.hourlyMap.put(new IntWritable(entry.getKey()), new IntWritable(entry.getValue().size()));
                }
                this.outputValue.setValue(this.hourlyMap);
                //输出操作
                context.write(key, this.outputValue);
            } else {
                //将uuid添加到set集合中，方便进行 统计uuid的去重个数
                for (TimeOutputValue value : values) {
                    unique.add(value.getId());
                }
                //设置kpi
                outputValue.setKpi(KpiType.valueOfName(kpiName));
                //设置value
                map.put(new IntWritable(-1), new IntWritable(this.unique.size()));
                outputValue.setValue(map);

                //进行输出
                context.write(key, outputValue);

            }

        } finally {
            //清空操作
            this.unique.clear();
            this.map.clear();
            this.hourlyMap.clear();
            this.hourlyUnique.clear();
            //初始化操作
            for (int i = 0; i < 24; i++) {
                this.hourlyMap.put(new IntWritable(i), new IntWritable(0));
                this.hourlyUnique.put(i, new HashSet<String>());
            }
        }
    }
}
