package com.beifeng.transformer.hive;

import com.beifeng.common.GlobalConstants;
import com.beifeng.transformer.model.dim.base.EventDimension;
import com.beifeng.transformer.service.rpc.IDimensionConverter;
import com.beifeng.transformer.service.rpc.client.DimensionConverterClient;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by zhouning on 2017/11/26.
 * desc: 计算event相关数据的udf
 */
public class EventDimensionUDF extends UDF {
    private IDimensionConverter converter = null;

    public EventDimensionUDF() {
        try {
            this.converter = DimensionConverterClient.createDimensionConverter(new Configuration());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("创建converter异常");
        }

        //添加一个钩子，进行关闭操作
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    DimensionConverterClient.stopDimensionConverterProxy(converter);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }));
    }


    /**
     * 根据给定的category和action获取对应的Id
     *
     * @param category event的category名称
     * @param action   event的action名称
     * @return
     */
    public IntWritable evaluate(Text category, Text action) {
        String ca = category.toString();
        String ac = action.toString();
        if (StringUtils.isBlank(ca)) {
            ca = GlobalConstants.DEFAULT_VALUE;
        }
        if (StringUtils.isBlank(ac)) {
            ac = GlobalConstants.DEFAULT_VALUE;
        }

        EventDimension dimension = new EventDimension(ca, ac);
        try {
            int id = converter.getDimensionIdByValue(dimension);
            return new IntWritable(id);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("获取id异常");
        }
    }
}
