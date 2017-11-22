package com.beifeng.transformer.mr.sessions;

import com.beifeng.common.DateEnum;
import com.beifeng.common.GlobalConstants;
import com.beifeng.common.KpiType;
import com.beifeng.transformer.model.dim.StatsUserDimension;
import com.beifeng.transformer.model.value.map.TimeOutputValue;
import com.beifeng.transformer.model.value.reduce.MapWritableValue;
import com.beifeng.transformer.util.TimeChain;
import com.beifeng.util.TimeUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.sql.Time;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhouning on 2017/11/17.
 * desc:
 */
public class SessionsReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, MapWritableValue> {

    private Map<String, TimeChain> timeChainMap = new HashMap<>();
    private MapWritableValue outputValue = new MapWritableValue();
    private MapWritable map = new MapWritable();

    private Map<Integer, Map<String, TimeChain>> hourlyTimeChainMap = new HashMap<>();//用于计算hourly的会话长度和会话时长
    private MapWritable hourlySessionsMap = new MapWritable();
    private MapWritable hourlySessionsLengthMap = new MapWritable();


    /**
     * 初始化方法
     */
    private void startUp() {
        //初始化操作
        this.map.clear();
        this.hourlySessionsMap.clear();
        this.hourlySessionsLengthMap.clear();
        this.timeChainMap.clear();
        this.hourlyTimeChainMap.clear();
        for (int i = 0; i < 24; i++) {
            this.hourlySessionsMap.put(new IntWritable(i), new IntWritable(0));
            this.hourlySessionsLengthMap.put(new IntWritable(i), new IntWritable(0));
            this.hourlyTimeChainMap.put(i, new HashMap<String, TimeChain>());
        }
    }

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {

        this.startUp();//初始化操作，清空
        String kpiName = key.getStatsCommon().getKpi().getKpiName();
        if (KpiType.SESSIONS.name.equals(kpiName)) {
            //计算stats_user表的sessions和session_length，同时也计算hourly_sessions&hourly_sessions_length
            this.handleSessions(key, values, context);
        } else if (KpiType.BROWSER_SESSIONS.name.equals(kpiName)) {
            //处理Browser维度的统计信息
            this.handleBrowserSessions(key, values, context);
        }

        for (TimeOutputValue value : values) {

            TimeChain chain = timeChainMap.get(value.getId());
            if (chain == null) {
                chain = new TimeChain(value.getTime());
                timeChainMap.put(value.getId(), chain);//保存
            }
            chain.addTime(value.getTime());//更新时间
        }


        //计算总的间隔秒数
        int sessionsLength = 0;
        //1. 计算间隔毫秒数
        for (Map.Entry<String, TimeChain> entry : timeChainMap.entrySet()) {
            long tmp = entry.getValue().getTimeOfMillis();
            if (tmp < 0 || tmp > GlobalConstants.DAY_OF_MILLISECONDS) {
                continue;
            }
            sessionsLength += tmp;
        }

        //2.计算间隔秒数，如果毫秒不足一秒，算作一秒
        if (sessionsLength % 1000 == 0) {
            sessionsLength = sessionsLength / 1000;
        } else {
            sessionsLength = sessionsLength / 1000 + 1;
        }

        //填充value
        this.map.put(new IntWritable(-1), new IntWritable(this.timeChainMap.size()));//填充 会话个数
        this.map.put(new IntWritable(-2), new IntWritable(sessionsLength));//会话长度
        this.outputValue.setValue(this.map);

        //填充kpi
        this.outputValue.setKpi(KpiType.valueOfName(kpiName));
        context.write(key, this.outputValue);
    }


    private void handleSessions(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        for (TimeOutputValue value : values) {
            String sid = value.getId();
            long time = value.getTime();

            //处理正常统计
            TimeChain chain = this.timeChainMap.get(sid);
            if (chain == null) {
                chain = new TimeChain(time);
                this.timeChainMap.put(sid, chain);//保存
            }
            chain.addTime(time);//更新时间

            //处理 hourly统计
            int hour = TimeUtil.getDateInfo(time, DateEnum.HOUR);
            Map<String, TimeChain> htcm = this.hourlyTimeChainMap.get(hour);
            TimeChain hourlyChain = htcm.get(sid);
            if (hourlyChain == null) {
                hourlyChain = new TimeChain(time);
                htcm.put(sid, hourlyChain);
                this.hourlyTimeChainMap.put(hour, htcm);
            }
            hourlyChain.addTime(time);//更新时间

        }

        //计算hourly的会话个数和会话长度信息
        for (Map.Entry<Integer, Map<String, TimeChain>> entry : this.hourlyTimeChainMap.entrySet()) {
            this.hourlySessionsMap.put(new IntWritable(entry.getKey()), new IntWritable(entry.getValue().size()));//设置当前小时的session个数

            int pres1 = 0;//统计每小时的总会话时长
            for (Map.Entry<String, TimeChain> entry2 : entry.getValue().entrySet()) {
                long tmp = entry2.getValue().getTimeOfMillis();//间隔毫秒数
                if (tmp < 0 || tmp > 3600000) {
                    //会话时长小于0或者大于1个小时
                    continue;
                }
                pres1 += tmp;
            }

            //计算间隔秒数，如果毫秒不足一秒，算作一秒
            if (pres1 % 1000 == 0) {
                pres1 = pres1 / 1000;
            } else {
                pres1 = pres1 / 1000 + 1;
            }
            this.hourlySessionsLengthMap.put(new IntWritable(entry.getKey()), new IntWritable(pres1));

        }

        //进行hourly sessions输出
        this.outputValue.setValue(this.hourlySessionsMap);
        //填充kpi
        this.outputValue.setKpi(KpiType.HOURLY_SESSIONS);
        context.write(key, this.outputValue);

        //进行hourly sessions length 输出
        this.outputValue.setValue(this.hourlySessionsLengthMap);
        //填充kpi
        this.outputValue.setKpi(KpiType.HOURLY_SESSIONS_LENGTH);
        context.write(key, this.outputValue);

        //计算正常的session和sessionsLength
        //计算总间隔秒数
        int sessionsLength = 0;
        //1. 计算间隔毫秒数
        for (Map.Entry<String, TimeChain> entry : this.timeChainMap.entrySet()) {
            long tmp = entry.getValue().getTimeOfMillis();//间隔毫秒数
            if (tmp < 0 || tmp > GlobalConstants.DAY_OF_MILLISECONDS) {
                //如果计算的值是小于0或者大于1天的毫秒数，直接过滤
                continue;
            }
            sessionsLength += tmp;
        }

        // 计算间隔秒数，如果毫秒不足一秒，算作一秒
        if (sessionsLength % 1000 == 0) {
            sessionsLength = sessionsLength / 1000;
        } else {
            sessionsLength = sessionsLength / 1000 + 1;
        }

        //填充value
        this.map.put(new IntWritable(-1), new IntWritable(this.timeChainMap.size()));//填充会话个数
        this.map.put(new IntWritable(-2), new IntWritable(sessionsLength));//会话长度
        this.outputValue.setValue(this.map);

        //填充Kpi
        this.outputValue.setKpi(KpiType.SESSIONS);
        context.write(key, this.outputValue);

    }

    private void handleBrowserSessions(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {

        for (TimeOutputValue value : values) {
            TimeChain chain = this.timeChainMap.get(value.getId());
            if (chain == null) {
                chain = new TimeChain(value.getTime());
                this.timeChainMap.put(value.getId(), chain);
            }
            chain.addTime(value.getTime());
        }


        //计算总的间隔秒数
        int sessionsLength = 0;
        //1. 计算间隔毫秒数
        for (Map.Entry<String, TimeChain> entry : this.timeChainMap.entrySet()) {
            long tmp = entry.getValue().getTimeOfMillis();
            if (tmp < 0 || tmp > GlobalConstants.DAY_OF_MILLISECONDS) {
                //如果计算的值小于0或者大于1天的毫秒数，直接过滤
                continue;
            }
            sessionsLength += tmp;
        }

        //2. 计算间隔秒数，如果毫秒不足1秒，算作一秒
        if (sessionsLength % 1000 == 0) {
            sessionsLength = sessionsLength / 1000;
        } else {
            sessionsLength = sessionsLength / 1000 + 1;
        }

        //填充value
        this.map.put(new IntWritable(-1), new IntWritable(this.timeChainMap.size()));//填充 会话个数
        this.map.put(new IntWritable(-2), new IntWritable(sessionsLength));//会话长度
        this.outputValue.setValue(this.map);
        //填充kpi
        this.outputValue.setKpi(KpiType.BROWSER_SESSIONS);
        context.write(key, this.outputValue);

    }
}
