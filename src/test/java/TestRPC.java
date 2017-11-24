import com.beifeng.transformer.model.dim.base.PlatformDimension;
import com.beifeng.transformer.service.rpc.IDimensionConverter;
import com.beifeng.transformer.service.rpc.client.DimensionConverterClient;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by Zhou Ning on 2017/11/24.
 * <p>
 * Desc:
 */
public class TestRPC {

    @Test
    public void test(){
        try {
            IDimensionConverter converter = DimensionConverterClient.createDimensionConverter(new Configuration());
            System.out.println(converter);
            System.out.println(converter.getDimensionIdByValue(new PlatformDimension("test")));
            DimensionConverterClient.stopDimensionConverterProxy(converter);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
} 