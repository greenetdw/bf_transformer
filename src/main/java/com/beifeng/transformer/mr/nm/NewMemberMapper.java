package com.beifeng.transformer.mr.nm;

import com.beifeng.common.DateEnum;
import com.beifeng.common.EventLogConstants;
import com.beifeng.common.GlobalConstants;
import com.beifeng.common.KpiType;
import com.beifeng.transformer.model.dim.StatsCommonDimension;
import com.beifeng.transformer.model.dim.StatsUserDimension;
import com.beifeng.transformer.model.dim.base.BrowserDimension;
import com.beifeng.transformer.model.dim.base.DateDimension;
import com.beifeng.transformer.model.dim.base.KpiDimension;
import com.beifeng.transformer.model.dim.base.PlatformDimension;
import com.beifeng.transformer.model.value.map.TimeOutputValue;
import com.beifeng.transformer.util.MemberUtil;
import com.beifeng.util.JdbcManager;
import com.beifeng.util.TimeUtil;
import com.sun.jersey.spi.container.servlet.WebConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;


public class NewMemberMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {
    private static final Logger logger = Logger.getLogger(NewMemberMapper.class);
    public static final byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);
    private StatsUserDimension outputKey = new StatsUserDimension();
    private TimeOutputValue outputValue = new TimeOutputValue();
    private BrowserDimension defaultBrowser = new BrowserDimension("", "");
    private KpiDimension newMemberKpi = new KpiDimension(KpiType.NEW_MEMBER.name);
    private KpiDimension newMemberOfBrowserKpi = new KpiDimension(KpiType.BROWSER_NEW_MEMBER.name);
    private Connection conn = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        //进行初始化操作
        Configuration conf = context.getConfiguration();
        try {
            this.conn = JdbcManager.getConnection(conf, GlobalConstants.WAREHOUSE_OF_REPORT);
            //删除制定日期的数据
            MemberUtil.deleteMemberInfoByDate(conf.get(GlobalConstants.RUNNING_DATE_PARAMES), conn);
        } catch (Exception e) {
            logger.error("获取 数据库连接出现异常", e);
            throw new RuntimeException("数据库连接信息获取失败", e);
        }
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        //获取会员id
        String memberId = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID)));
        //判断memberId是否是第一次访问
        try {
            if (StringUtils.isBlank(memberId) || !MemberUtil.isValidateMemberId(memberId) || MemberUtil.isNewMemberId(memberId, this.conn)) {
                logger.warn("memberId不能为空，而且需要是第一次访问网站的会员id");
                return;
            }
        } catch (SQLException e) {
            logger.error("查询会员id是否是新会晕啊id出现数据异常", e);
            throw new IOException("查询数据库出现异常", e);
        }

        //memberId是第一次访问，获取平台名称、服务器时间
        String platform = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)));
        String serverTime = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)));

        //过滤无效数据
        if (StringUtils.isBlank(platform) || StringUtils.isBlank(serverTime) || !StringUtils.isNumeric(serverTime.trim())) {
            logger.warn("平台名称&服务器时间不能为空，而且服务器时间必须为时间戳形式");
            return;
        }

        long longOfServerTime = Long.valueOf(serverTime.trim());
        DateDimension dateDimension = DateDimension.buildDate(longOfServerTime, DateEnum.DAY);

        //创建platform维度信息
        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);

        //创建browser维度信息
        String browserName = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME)));
        String browserVersion = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION)));
        List<BrowserDimension> browserDimensions = BrowserDimension.buildList(browserName, browserVersion);

        //设置输出
        this.outputValue.setId(memberId);
        StatsCommonDimension statsCommon = this.outputKey.getStatsCommon();
        statsCommon.setDate(dateDimension);

        for (PlatformDimension pf : platformDimensions) {
            //基本信息输出
            this.outputKey.setBrowser(defaultBrowser);//设置一个默认值，方便进行控制
            statsCommon.setKpi(newMemberKpi);
            statsCommon.setPlatform(pf);
            context.write(this.outputKey, this.outputValue);

            //浏览器信息输出
            statsCommon.setKpi(newMemberOfBrowserKpi);
            for (BrowserDimension bw : browserDimensions) {
                this.outputKey.setBrowser(bw);
                context.write(this.outputKey, this.outputValue);
            }
        }
    }
}
