package com.beifeng.transformer.mr.nm;

import com.beifeng.common.KpiType;
import com.beifeng.transformer.model.dim.StatsUserDimension;
import com.beifeng.transformer.model.value.map.TimeOutputValue;
import com.beifeng.transformer.model.value.reduce.MapWritableValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by zhouning on 2017/11/16.
 * desc:
 */
public class NewMemberReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, MapWritableValue> {

    private Set<String> unique = new HashSet<String>();
    private MapWritableValue outputValue = new MapWritableValue();
    private MapWritable map = new MapWritable();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {

        for (TimeOutputValue value : values) {
            this.unique.add(value.getId());
        }

        //输出memberId
        this.outputValue.setKpi(KpiType.INSERT_MEMBER_INFO);
        for (String memberId : this.unique) {
            this.map.put(new IntWritable(-1), new Text(memberId));
            this.outputValue.setValue(this.map);
            context.write(key, this.outputValue);
        }


        //value指定
        this.map.put(new IntWritable(-1), new IntWritable(this.unique.size()));
        this.outputValue.setValue(this.map);

        //kpi指定
        this.outputValue.setKpi(KpiType.valueOfName(key.getStatsCommon().getKpi().getKpiName()));
        context.write(key, this.outputValue);

    }
}
