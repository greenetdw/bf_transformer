package com.beifeng.transformer.hive;

import com.beifeng.transformer.model.dim.base.PlatformDimension;
import com.beifeng.transformer.service.rpc.IDimensionConverter;
import com.beifeng.transformer.service.rpc.client.DimensionConverterClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by Zhou Ning on 2017/11/24.
 * Desc: 操作平台dimension 相关的UDF
 */
public class PlatformDimensionUDF extends UDF {

    private IDimensionConverter converter = null;

    public PlatformDimensionUDF() {
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
     * 根据给定的平台名称返回对应的id
     *
     * @param platformName
     * @return
     */
    public IntWritable evaluate(Text platformName) {
        PlatformDimension dimension = new PlatformDimension(platformName.toString());
        try {
            int id = this.converter.getDimensionIdByValue(dimension);
            return new IntWritable(id);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("获取Id异常");
        }
    }


} 