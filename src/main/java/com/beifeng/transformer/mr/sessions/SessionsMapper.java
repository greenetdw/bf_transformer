package com.beifeng.transformer.mr.sessions;

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
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhouning on 2017/11/17.
 * desc:
 */
public class SessionsMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {

    private static Logger logger = Logger.getLogger(SessionsMapper.class);

    private StatsUserDimension outputKey = new StatsUserDimension();
    private TimeOutputValue outputValue = new TimeOutputValue();
    private BrowserDimension defaultBrowser = new BrowserDimension("", "");

    public static final byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);
    private KpiDimension sessionsKpi = new KpiDimension(KpiType.SESSIONS.name);
    private KpiDimension sessionsOfBrowserKpi = new KpiDimension(KpiType.BROWSER_SESSIONS.name);

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        //获取会话id，serverTime，平台
        String sessionId = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SESSION_ID)));
        String serverTime = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)));
        String platform = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)));

        //过滤无效数据
        if (StringUtils.isBlank(sessionId) || StringUtils.isBlank(platform) || StringUtils.isBlank(serverTime) || !StringUtils.isNumeric(serverTime.trim())) {
            logger.warn("会话id&platform&服务器时间不能为空，而且服务器时间必须为时间戳格式");
            return;
        }

        //创建date维度
        long longOfServerTime = Long.valueOf(serverTime);
        DateDimension dateDimension = DateDimension.buildDate(longOfServerTime, DateEnum.DAY);
        //创建platform
        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);

        //创建browser维度
        String browserName = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME)));
        String browserVersion = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION)));
        List<BrowserDimension> browserDimensions = BrowserDimension.buildList(browserName, browserVersion);


        //进行输出设置
        this.outputValue.setId(sessionId);//会话id
        this.outputValue.setTime(longOfServerTime);//服务器时间

        StatsCommonDimension statsCommon = this.outputKey.getStatsCommon();
        statsCommon.setDate(dateDimension);
        for (PlatformDimension pf : platformDimensions) {
            this.outputKey.setBrowser(defaultBrowser);
            statsCommon.setKpi(sessionsKpi);
            statsCommon.setPlatform(pf);
            context.write(this.outputKey, this.outputValue);//输出设置

            //browser输出
            statsCommon.setKpi(sessionsOfBrowserKpi);//将kpi更改为输出 browser session
            for (BrowserDimension br : browserDimensions) {
                this.outputKey.setBrowser(br);
                context.write(this.outputKey, this.outputValue);
            }

        }

    }
}
