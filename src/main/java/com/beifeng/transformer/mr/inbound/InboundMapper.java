package com.beifeng.transformer.mr.inbound;

import com.beifeng.common.DateEnum;
import com.beifeng.common.KpiType;
import com.beifeng.transformer.model.dim.StatsCommonDimension;
import com.beifeng.transformer.model.dim.StatsInboundDimension;
import com.beifeng.transformer.model.dim.base.DateDimension;
import com.beifeng.transformer.model.dim.base.KpiDimension;
import com.beifeng.transformer.model.dim.base.PlatformDimension;
import com.beifeng.transformer.model.value.map.TextsOutputValue;
import com.beifeng.transformer.mr.TransformerBaseMapper;
import com.beifeng.transformer.service.impl.InboundDimensionService;
import com.beifeng.transformer.util.UrlUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by zhouning on 2017/11/22.
 * desc: 统计inbound相关的活跃用户和总会话个数的一个mapper类<br/>
 * 输入：platform, serverTime, referrer url, uuid, sid<br/>
 */
public class InboundMapper extends TransformerBaseMapper<StatsInboundDimension, TextsOutputValue> {

    private static final Logger logger = Logger.getLogger(InboundMapper.class);
    private StatsInboundDimension outputKey = new StatsInboundDimension();
    private TextsOutputValue outputValue = new TextsOutputValue();
    private KpiDimension inboundKpiDimension = new KpiDimension(KpiType.INBOUND.name);
    private Map<String, Integer> inbounds = null;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        try {
            //获取inbound相关数据
            this.inbounds = InboundDimensionService.getInboundByType(context.getConfiguration(), 0);
        } catch (Exception e) {
            logger.error("获取外链id出现数据库异常", e);
            throw new RuntimeException("出现异常", e);
        }
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        this.inputRecords++;

        //获取数据
        String platform = this.getPlatform(value);
        String serverTime = this.getServerTime(value);
        String referrerUrl = this.getReferrerUrl(value);
        String uuid = this.getUuid(value);
        String sid = this.getSessionId(value);

        //过滤无效数据
        if (StringUtils.isBlank(platform) || StringUtils.isBlank(serverTime) || StringUtils.isBlank(referrerUrl) || StringUtils.isBlank(uuid) || StringUtils.isBlank(sid) || !StringUtils.isNumeric(serverTime.trim())) {
            logger.warn("平台&uuid&&会话id&前一个页面的url和服务器时间不能为空，而且服务器时间必须为时间戳形式");
            this.filterRecords++;
            return;
        }

        //转换url为外链id
        int inboundId = 0;
        try {
            inboundId = getInboundIdByHost(UrlUtil.getHost(referrerUrl));
        } catch (Exception e) {
            logger.warn("获取referrer url 对应的inbound id异常");
            inboundId = 0;
        }
        //过滤无效的inbound id
        if (inboundId <= 0) {
            //如果获取的inbound id 小于等于0，那么表示无效inbound
            logger.warn("该url对应的不是外链url:" + referrerUrl);
            this.filterRecords++;
            return;
        }

        //构建platform维度
        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);
        //构建输出对象
        this.outputValue.setSid(sid);
        this.outputValue.setUuid(uuid);

        StatsCommonDimension statsCommon = this.outputKey.getStatsCommon();
        statsCommon.setDate(DateDimension.buildDate(Long.valueOf(serverTime.trim()), DateEnum.DAY));
        statsCommon.setKpi(inboundKpiDimension);

        //输出
        for (PlatformDimension pf : platformDimensions) {
            statsCommon.setPlatform(pf);

            //输出全部inbound维度
            this.outputKey.getInbound().setId(InboundDimensionService.ALL_OF_INBOUND_ID);
            context.write(this.outputKey, this.outputValue);
            this.outputRecrods++;

            //输出具体Inbound维度
            this.outputKey.getInbound().setId(inboundId);
            context.write(this.outputKey, this.outputValue);
            this.outputRecrods++;
        }

    }

    /**
     * 根据url的host来获取不同的inbound id值
     * 如果该host是统计网站本身host，那么直接返回0，也就是说，如果host不属于外链，那么返回0
     *
     * @param host
     * @return
     */
    private int getInboundIdByHost(String host) {
        int id = 0;
        if (UrlUtil.isValidateInboundHost(host)) {
            //是一个有效的外链host，那么进行inbound id获取操作
            id = InboundDimensionService.OTHER_OF_INBOUND_ID;//预先设定为其他外链

            //查看是否是一个具体的inbound id值
            for (Map.Entry<String, Integer> entry : this.inbounds.entrySet()) {
                String urlRegex = entry.getKey();
                if (host.equals(urlRegex) || host.startsWith(urlRegex) || host.matches(urlRegex)) {
                    id = entry.getValue();
                    break;
                }
            }

        }
        return id;
    }

}
