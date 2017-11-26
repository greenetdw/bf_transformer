package com.beifeng.transformer.hive;

import com.beifeng.common.GlobalConstants;
import com.beifeng.transformer.model.dim.base.PaymentTypeDimension;
import com.beifeng.transformer.service.rpc.IDimensionConverter;
import com.beifeng.transformer.service.rpc.client.DimensionConverterClient;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by zhouning on 2017/11/26.
 * desc:订单支付方式dimension对应的udf
 */
public class PaymentTypeDimensionUDF extends UDF {

    private IDimensionConverter converter = null;

    public PaymentTypeDimensionUDF() {
        try {
            this.converter = DimensionConverterClient.createDimensionConverter(new Configuration());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("创建converter异常");
        }

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                DimensionConverterClient.stopDimensionConverterProxy(converter);
            }
        }));
    }

    /***
     * 根据给定的payment方式名称，返回对应的id值
     * @param paymentType
     * @return
     */
    public int evaluate(String paymentType) {
        paymentType = StringUtils.isBlank(paymentType) ? GlobalConstants.DEFAULT_VALUE : paymentType.trim();
        PaymentTypeDimension dimension = new PaymentTypeDimension(paymentType);
        try {
            return this.converter.getDimensionIdByValue(dimension);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("获取id异常");
        }
    }

}
