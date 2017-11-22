package com.beifeng.transformer.mr.locations;

import com.beifeng.common.DateEnum;
import com.beifeng.common.KpiType;
import com.beifeng.transformer.model.dim.StatsCommonDimension;
import com.beifeng.transformer.model.dim.StatsLocationDimension;
import com.beifeng.transformer.model.dim.base.DateDimension;
import com.beifeng.transformer.model.dim.base.KpiDimension;
import com.beifeng.transformer.model.dim.base.LocationDimension;
import com.beifeng.transformer.model.dim.base.PlatformDimension;
import com.beifeng.transformer.model.value.map.TextsOutputValue;
import com.beifeng.transformer.mr.TransformerBaseMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhouning on 2017/11/21.
 * desc:统计location维度信息的mapper类<br/>
 * 输入：country, province, city, platform, serverTime, uuid, sid<br/>
 * 一个输入对应6条输出
 */
public class LocationMapper extends TransformerBaseMapper<StatsLocationDimension, TextsOutputValue> {

    private static final Logger logger = Logger.getLogger(LocationMapper.class);
    private StatsLocationDimension outputKey = new StatsLocationDimension();
    private TextsOutputValue outputValue = new TextsOutputValue();
    private KpiDimension locationKpiDimension = new KpiDimension(KpiType.LOCATION.name);

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        this.inputRecords++;

        //获取平台名称，服务器时间，用户id，会话id
        String platform = this.getPlatform(value);
        String serverTime = this.getServerTime(value);
        String uuid = this.getUuid(value);
        String sid = this.getSessionId(value);

        //过滤无效数据
        if (StringUtils.isBlank(platform) || StringUtils.isBlank(uuid) || StringUtils.isBlank(sid) || StringUtils.isBlank(serverTime) || !StringUtils.isNumeric(serverTime.trim())) {
            logger.warn("平台&uuid&&会话id&服务器时间不能为空，而且服务器时间必须为时间戳类型");
            this.filterRecords++;
        }

        //时间维度创建
        long longOfServerTime = Long.valueOf(serverTime);
        DateDimension dateDimension = DateDimension.buildDate(longOfServerTime, DateEnum.DAY);


        //platform维度创建
        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);

        //location维度创建
        String country = this.getCountry(value);
        String province = this.getProvince(value);
        String city = this.getCity(value);
        List<LocationDimension> locationDimensions = LocationDimension.buildList(country, province, city);

        //进行输出定义
        this.outputValue.setUuid(uuid);
        this.outputValue.setSid(sid);
        StatsCommonDimension statsCommon = this.outputKey.getStatsCommon();
        statsCommon.setDate(dateDimension);
        statsCommon.setKpi(this.locationKpiDimension);
        for (PlatformDimension pf : platformDimensions) {
            statsCommon.setPlatform(pf);

            for (LocationDimension location : locationDimensions) {
                this.outputKey.setLocation(location);
                context.write(this.outputKey, this.outputValue);
                this.outputRecrods++;
            }
        }
    }
}
