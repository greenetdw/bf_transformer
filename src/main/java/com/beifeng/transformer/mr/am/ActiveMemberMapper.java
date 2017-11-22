package com.beifeng.transformer.mr.am;

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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.awt.*;
import java.io.IOException;
import java.util.*;
import java.util.List;

/**
 * mapreduce程序中计算active member的mapper类<br/>
 * 其实就是按照维度信息进行分组输出
 */
public class ActiveMemberMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {

    private static final Logger logger = Logger.getLogger(ActiveMemberMapper.class);
    public static final byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);
    private StatsUserDimension outputKey = new StatsUserDimension();
    private TimeOutputValue outputValue = new TimeOutputValue();
    private BrowserDimension defaultBrowser = new BrowserDimension("", "");
    private KpiDimension activeMemberKpi = new KpiDimension(KpiType.ACTIVE_MEMBER.name);
    private KpiDimension activeMemberOfBrowserKpi = new KpiDimension(KpiType.BROWSER_ACTIVE_MEMBER.name);

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //获取u_mid，platform, serverTime，从hbase返回的结果集Result中
        String memberId = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID)));
        String platform = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)));
        String serverTime = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)));

        //过滤无效数据
        if (StringUtils.isBlank(memberId) || StringUtils.isBlank(platform) || StringUtils.isBlank(serverTime) || !StringUtils.isNumeric(serverTime.trim())) {
            System.out.println("---------> row key : " + Bytes.toString(value.getRow()));
            logger.warn("memberId & platform & serverTime 不能为空，而且serverTime必须是时间戳");
            return;
        }

        long longOfServerTime = Long.valueOf(serverTime);
        DateDimension dateDimension = DateDimension.buildDate(longOfServerTime, DateEnum.DAY);
        this.outputValue.setId(memberId);

        //platform的构建
        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);
        //获取browser name和browser version
        String browserName = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME)));
        String browserVersion = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION)));

        //进行browser的维度的信息构建
        List<BrowserDimension> browserDimensions = BrowserDimension.buildList(browserName, browserVersion);


        //开始进行输出
        StatsCommonDimension statsCommon = this.outputKey.getStatsCommon();
        //设置date dimension
        statsCommon.setDate(dateDimension);
        for (PlatformDimension pf : platformDimensions) {
            this.outputKey.setBrowser(defaultBrowser);//进行覆盖操作
            //设置kpi dimension
            statsCommon.setKpi(activeMemberKpi);
            //设置platform dimension
            statsCommon.setPlatform(pf);
            context.write(this.outputKey, this.outputValue);

            //输出browser维度统计
            statsCommon.setKpi(activeMemberOfBrowserKpi);
            for (BrowserDimension bw : browserDimensions) {
                this.outputKey.setBrowser(bw);//设置对应的browsers
                context.write(this.outputKey, this.outputValue);
            }
        }

    }
}
