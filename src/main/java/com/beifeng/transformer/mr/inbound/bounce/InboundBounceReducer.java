package com.beifeng.transformer.mr.inbound.bounce;

import com.beifeng.common.KpiType;
import com.beifeng.transformer.model.dim.StatsCommonDimension;
import com.beifeng.transformer.model.dim.StatsInboundBounceDimension;
import com.beifeng.transformer.model.dim.StatsInboundDimension;
import com.beifeng.transformer.model.value.reduce.InboundBounceReduceValue;
import com.beifeng.transformer.service.impl.InboundDimensionService;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhouning on 2017/11/22.
 * desc:统计外链跳出会话的Reducer类
 */
public class InboundBounceReducer extends Reducer<StatsInboundBounceDimension, IntWritable, StatsInboundDimension, InboundBounceReduceValue> {

    private StatsInboundDimension outputKey = new StatsInboundDimension();

    @Override
    protected void reduce(StatsInboundBounceDimension key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String preSid = "";//前一条记录的会话id
        String curSid = "";//当前会话di

        //当前inbound id
        int curInboundId = InboundBounceMapper.DEFAULT_INBOUND_ID;
        //前一个记录的inbound id
        int preInbouncId = InboundBounceMapper.DEFAULT_INBOUND_ID;
        boolean isBounceVisit = true;//true表示是一个跳出会话，否则不是跳出会话
        boolean isNewSession = true;//表示是一个新的会话

        Map<Integer, InboundBounceReduceValue> map = new HashMap<>();
        map.put(InboundDimensionService.ALL_OF_INBOUND_ID, new InboundBounceReduceValue());//给一个默认值

        for (IntWritable value : values) {
            curSid = key.getSid();
            curInboundId = value.get();

            //同一个会话，而且当前inbound id 为0，那么一定是一个非跳出的visit
            if (curSid.equals(preSid) && (curInboundId == InboundBounceMapper.DEFAULT_INBOUND_ID)) {
                isBounceVisit = false;//该会话不是一个跳出会话
                continue;
            }

            //总的两种情况
            //1. 一个新会话
            //2. 一个非0的inbound id

            //表示是一个新会话或者是一个新的inbound，检查上一个inbound是否是一个跳出的inbound
            if (preInbouncId != InboundBounceMapper.DEFAULT_INBOUND_ID && isBounceVisit) {
                //针对上一个Inbound id需要进行一次跳出会话更新操作
                map.get(preInbouncId).incrBounceNumber();

                //针对all维度的bounce number的计算
                //规则1：一次会话中只要出现一次跳出外链，就将其算到all维度的跳出会话中去，只要出现一次跳出就算作跳出
                if (!curSid.equals(preSid)) {
                    map.get(InboundDimensionService.ALL_OF_INBOUND_ID).incrBounceNumber();
                }
            }

            //会话结束或者是当前inbound id 不为0，而且和前一个inbound id 不相等
            isBounceVisit = true;
            preInbouncId = InboundBounceMapper.DEFAULT_INBOUND_ID;

            //如果inbound是一个新的
            if (curInboundId != InboundBounceMapper.DEFAULT_INBOUND_ID) {
                preInbouncId = curInboundId;
                InboundBounceReduceValue irv = map.get(curInboundId);
                if (irv == null) {
                    irv = new InboundBounceReduceValue(0);
                    map.put(curInboundId, irv);
                }
            }

            //如果是一个新的会话，那么更新会话
            if (!preSid.equals(curSid)) {
                isNewSession = true;
                preSid = curSid;
            } else {
                isNewSession = false;
            }
        }

        //单独的处理最后一条数据
        //表示是一个新会话或者是一个新的inbound,检查上一个Inbound 是否是一个跳出的inbound
        if (preInbouncId != InboundBounceMapper.DEFAULT_INBOUND_ID && isBounceVisit) {
            //针对上一个inbound id 需要进行一次跳出会话更新操作
            map.get(preInbouncId).incrBounceNumber();

            //针对all维度的bounce number的计算
            //规则1：一次会话中只要出现一次跳出外链，将将其算到all维度的跳出会话中去，只要出现一次跳出就算做跳出
            if (isNewSession) {
                map.get(InboundDimensionService.ALL_OF_INBOUND_ID).incrBounceNumber();
            }
        }

        //数据 输出
        this.outputKey.setStatsCommon(StatsCommonDimension.clone(key.getStatsCommon()));
        for (Map.Entry<Integer, InboundBounceReduceValue> entry : map.entrySet()) {
            InboundBounceReduceValue value = entry.getValue();
            value.setKpi(KpiType.valueOfName(key.getStatsCommon().getKpi().getKpiName()));
            this.outputKey.getInbound().setId(entry.getKey());
            context.write(this.outputKey, value);
        }
    }
}
