package com.beifeng.transformer.hive;


import com.beifeng.common.DateEnum;
import com.beifeng.transformer.model.dim.base.DateDimension;
import com.beifeng.transformer.service.rpc.IDimensionConverter;
import com.beifeng.transformer.service.rpc.client.DimensionConverterClient;
import com.beifeng.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by Zhou Ning on 2017/11/24.
 * <p>
 * Desc:操作日期dimension 相关的udf
 */
public class DateDimensionUDF extends UDF {

    private IDimensionConverter converter = null;

    public DateDimensionUDF() {
        try {
            this.converter = DimensionConverterClient.createDimensionConverter(new Configuration());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("创建converter异常");
        }
        //添加一个钩子进行关闭操作
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                DimensionConverterClient.stopDimensionConverterProxy(converter);
            }
        }));
    }

    /**
     * 根据给定的日期(格式为：yyyy-MM-dd) 只返回id
     *
     * @param day
     * @return
     */
    public IntWritable evaluate(Text day) {
        DateDimension dimension = DateDimension.buildDate(TimeUtil.parseString2Long(day.toString()), DateEnum.DAY);
        try {
            int id = this.converter.getDimensionIdByValue(dimension);
            return new IntWritable(id);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("获取id异常");
        }
    }


} 