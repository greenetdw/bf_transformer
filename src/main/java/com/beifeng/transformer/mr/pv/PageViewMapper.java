package com.beifeng.transformer.mr.pv;

import com.beifeng.common.DateEnum;
import com.beifeng.common.EventLogConstants;
import com.beifeng.common.KpiType;
import com.beifeng.transformer.model.dim.StatsCommonDimension;
import com.beifeng.transformer.model.dim.StatsUserDimension;
import com.beifeng.transformer.model.dim.base.BrowserDimension;
import com.beifeng.transformer.model.dim.base.DateDimension;
import com.beifeng.transformer.model.dim.base.KpiDimension;
import com.beifeng.transformer.model.dim.base.PlatformDimension;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.awt.*;
import java.io.IOException;
import java.util.*;
import java.util.List;

/**
 * Created by zhouning on 2017/11/20.
 * desc: 统计pv的mapper类<br/>
 * 输入的hbase的数据，包括：platform,serverTime,browserName,browserVersion,url<br/>
 * 输出<StatsUserDimension,NullWritable>键值对，输出key中包含platform,date以及browser的维度信息
 */
public class PageViewMapper extends TableMapper<StatsUserDimension, NullWritable> {

    private static final Logger logger = Logger.getLogger(PageViewMapper.class);
    public static final byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);
    private StatsUserDimension outputKey = new StatsUserDimension();
    private KpiDimension websitPageViewDimension = new KpiDimension(KpiType.WEBSITE_PAGEVIEW.name);

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        //1. 获取platform, time, url
        String platform = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)));
        String serverTime = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)));
        String url = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_CURRENT_URL)));

        //过滤数据
        if (StringUtils.isBlank(platform) || StringUtils.isBlank(url) || StringUtils.isBlank(serverTime) || !StringUtils.isNumeric(serverTime.trim())) {
            logger.warn("平台&服务时间&当前url不能为空，而且服务器时间必须为时间戳形式的字符串");
            return;
        }

        //3.创建platform维度的信息
        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);
        //4. 创建browser维度的信息
        String browserName = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME)));
        String browserVersion = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION)));
        List<BrowserDimension> browserDimensions = BrowserDimension.buildList(browserName, browserVersion);
        //5. 创建date维度信息
        DateDimension dateDimension = DateDimension.buildDate(Long.valueOf(serverTime), DateEnum.DAY);

        //6.输出
        StatsCommonDimension statsCommon = this.outputKey.getStatsCommon();
        statsCommon.setDate(dateDimension);//设置date dimension
        statsCommon.setKpi(this.websitPageViewDimension);//设置kpi dimension
        for (PlatformDimension pf : platformDimensions) {
            statsCommon.setPlatform(pf);//设置platform dimension
            for (BrowserDimension bw : browserDimensions) {
                this.outputKey.setBrowser(bw);//设置browser dimension
                context.write(this.outputKey, NullWritable.get());
            }
        }
    }
}
