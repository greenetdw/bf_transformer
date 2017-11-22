package com.beifeng.transformer.mr.inbound.bounce;

import com.beifeng.common.DateEnum;
import com.beifeng.common.KpiType;
import com.beifeng.transformer.model.dim.StatsCommonDimension;
import com.beifeng.transformer.model.dim.StatsInboundBounceDimension;
import com.beifeng.transformer.model.dim.base.DateDimension;
import com.beifeng.transformer.model.dim.base.KpiDimension;
import com.beifeng.transformer.model.dim.base.PlatformDimension;
import com.beifeng.transformer.mr.TransformerBaseMapper;
import com.beifeng.transformer.service.impl.InboundDimensionService;
import com.beifeng.transformer.util.UrlUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by zhouning on 2017/11/22.
 * desc:计算外链跳出会话的会话个数
 */
public class InboundBounceMapper extends TransformerBaseMapper<StatsInboundBounceDimension, IntWritable> {

    /**
     * 默认inbound id ，用于标识不是外链
     */
    public static final int DEFAULT_INBOUND_ID = 0;
    private static final Logger logger = Logger.getLogger(InboundBounceMapper.class);
    private StatsInboundBounceDimension outputKey = new StatsInboundBounceDimension();
    private IntWritable outputValue = new IntWritable();
    private KpiDimension inboundBounceKpi = new KpiDimension(KpiType.INBOUND_BOUNCE.name);

    private Map<String, Integer> inbounds = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        try {
            //获取inbound相关数据
            inbounds = InboundDimensionService.getInboundByType(context.getConfiguration(), 0);
        } catch (Exception e) {
            logger.error("获取外链id出现数据库异常", e);
            throw new IOException("出现异常", e);
        }

    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        this.inputRecords++;

        //获取platform，serverTime, referrer url, sid
        String platform = this.getPlatform(value);
        String serverTime = this.getServerTime(value);
        String referrerUrl = this.getReferrerUrl(value);
        String sid = this.getSessionId(value);

        //过滤
        if (StringUtils.isBlank(platform) || StringUtils.isBlank(serverTime) || StringUtils.isBlank(referrerUrl) || StringUtils.isBlank(sid) || !StringUtils.isNumeric(serverTime.trim())) {
            this.filterRecords++;
            logger.warn("平台&服务时间&前一个页面的url&会话id不能为空，而且服务器时间必须是时间戳形式");
            return;
        }

        //创建platform
        long longOfServerTime = Long.valueOf(serverTime);
        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);

        //创建date
        DateDimension dateDimension = DateDimension.buildDate(longOfServerTime, DateEnum.DAY);

        //构建Inbound:转换url为外链id
        int inboundId = DEFAULT_INBOUND_ID;
        try {
            inboundId = this.getInboundIdByHost(UrlUtil.getHost(referrerUrl));
        } catch (Exception e) {
            logger.warn("获取referrer url对应的inbound id异常", e);
            inboundId = DEFAULT_INBOUND_ID;
        }

        //输出定义
        this.outputValue.set(inboundId);

        StatsCommonDimension statsCommon = this.outputKey.getStatsCommon();
        statsCommon.setDate(dateDimension);
        statsCommon.setKpi(this.inboundBounceKpi);
        this.outputKey.setSid(sid);
        this.outputKey.setServerTime(longOfServerTime);

        for (PlatformDimension pf : platformDimensions) {
            statsCommon.setPlatform(pf);
            context.write(this.outputKey, this.outputValue);
            this.outputRecrods++;
        }
    }


    /**
     * 根据url的host来获取不同的inbound id值，如果该host是统计网站本身的host，那么直接返回0，也就是如果host不属于外链，那么返回0
     *
     * @param host
     * @return
     */
    private int getInboundIdByHost(String host) {
        int id = DEFAULT_INBOUND_ID;
        if (UrlUtil.isValidateInboundHost(host)) {
            //是一个有效的外链host,那么进行Inbound id获取操作
            id = InboundDimensionService.OTHER_OF_INBOUND_ID;

            //查看是否是一个具体的inbound id值
            for (Map.Entry<String, Integer> entry : this.inbounds.entrySet()) {
                String urlRegx = entry.getKey();
                if (host.equals(urlRegx) || host.startsWith(urlRegx) || host.matches(urlRegx)) {
                    id = entry.getValue();
                    break;
                }
            }
        }
        return id;
    }
}
