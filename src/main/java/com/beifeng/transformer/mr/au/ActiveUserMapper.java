package com.beifeng.transformer.mr.au;

import com.beifeng.common.DateEnum;
import com.beifeng.common.EventLogConstants;
import com.beifeng.common.KpiType;
import com.beifeng.transformer.model.dim.StatsCommonDimension;
import com.beifeng.transformer.model.dim.StatsUserDimension;
import com.beifeng.transformer.model.dim.base.BrowserDimension;
import com.beifeng.transformer.model.dim.base.DateDimension;
import com.beifeng.transformer.model.dim.base.KpiDimension;
import com.beifeng.transformer.model.dim.base.PlatformDimension;
import com.beifeng.transformer.model.value.map.TimeOutputValue;
import com.beifeng.transformer.mr.TransformerBaseMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

public class ActiveUserMapper extends TransformerBaseMapper<StatsUserDimension, TimeOutputValue> {
    private static Logger logger = Logger.getLogger(ActiveUserMapper.class);
    private byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);
    private StatsUserDimension outputKey = new StatsUserDimension();
    private TimeOutputValue outputValue = new TimeOutputValue();
    private BrowserDimension defaultBrowser = new BrowserDimension("", "");//默认的browser对象

    private KpiDimension activeUserKpi = new KpiDimension(KpiType.ACTIVE_USER.name);
    private KpiDimension activeUserOfBrowserKpi = new KpiDimension(KpiType.BROWSER_ACTIVE_USER.name);

    private KpiDimension hourlyActiveUserKpi = new KpiDimension(KpiType.HOURLY_ACTIVE_USER.name);

    private String uuid, platform, serverTime, browserName, browserVersion;

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        this.inputRecords++;
        //获取uuid,platform,serverTime,从hbase返回的结果集中
        this.uuid = this.getUuid(value);
        this.platform = this.getPlatform(value);
        this.serverTime = this.getServerTime(value);

        //过滤无效数据
        if (StringUtils.isBlank(uuid) || StringUtils.isBlank(platform) || StringUtils.isBlank(serverTime) || !StringUtils.isNumeric(serverTime.trim())) {
            logger.warn("uuid&platform&serverTime不能为空，而且serverTime必须为时间戳");
            this.filterRecords++;
            return;
        }

        Long longOfServerTime = Long.valueOf(serverTime.trim());
        DateDimension dateDimension = DateDimension.buildDate(longOfServerTime, DateEnum.DAY);
        this.outputValue.setId(uuid);
        this.outputValue.setTime(longOfServerTime);//设置访问的服务器时间，可以用来计算该用户访问的时间是哪个时间段

        //进行platform创建
        List<PlatformDimension> platforms = PlatformDimension.buildList(platform);

        this.browserName = this.getBrowserName(value);
        this.browserVersion = this.getBrowserVersion(value);
        List<BrowserDimension> browserDimensions = BrowserDimension.buildList(browserName, browserVersion);

        StatsCommonDimension statsCommon = this.outputKey.getStatsCommon();
        statsCommon.setDate(dateDimension);
        for (PlatformDimension pf : platforms) {
            this.outputKey.setBrowser(defaultBrowser);
            statsCommon.setPlatform(pf);
            //输出active user的键值对
            //设置kpi dimension
            statsCommon.setKpi(activeUserKpi);
            context.write(outputKey, outputValue);
            this.outputRecrods++;

            //输出hourly active user的键值对
            statsCommon.setKpi(hourlyActiveUserKpi);
            context.write(this.outputKey, this.outputValue);
            this.outputRecrods++;

            //输出browser维度统计
            statsCommon.setKpi(activeUserOfBrowserKpi);
            for (BrowserDimension bw : browserDimensions) {
                this.outputKey.setBrowser(bw);//设置对应的browsers
                context.write(this.outputKey, this.outputValue);
                this.outputRecrods++;
            }
        }

    }
}
