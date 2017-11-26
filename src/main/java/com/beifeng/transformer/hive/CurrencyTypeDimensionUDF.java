package com.beifeng.transformer.hive;

import com.beifeng.common.GlobalConstants;
import com.beifeng.transformer.model.dim.base.CurrencyTypeDimension;
import com.beifeng.transformer.service.rpc.IDimensionConverter;
import com.beifeng.transformer.service.rpc.client.DimensionConverterClient;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by zhouning on 2017/11/26.
 * desc:订单支付货币类型dimension操作udf
 */
public class CurrencyTypeDimensionUDF extends UDF {

    private IDimensionConverter converter = null;

    public CurrencyTypeDimensionUDF() {
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
     * 根据给定的currency 名称，返回对应的id值
     *
     * @param currencyName
     * @return
     */
    public int evaluate(String currencyName) {
        currencyName = StringUtils.isBlank(currencyName) ? GlobalConstants.DEFAULT_VALUE : currencyName.trim();
        CurrencyTypeDimension dimension = new CurrencyTypeDimension(currencyName);
        try {
            return this.converter.getDimensionIdByValue(dimension);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("获取id异常");
        }
    }
}
