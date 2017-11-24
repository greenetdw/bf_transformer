package com.beifeng.transformer.service.rpc.client;

import com.beifeng.transformer.model.dim.base.BaseDimension;
import com.beifeng.transformer.service.rpc.IDimensionConverter;
import com.beifeng.transformer.service.rpc.server.DimensionConverterImpl;
import com.beifeng.transformer.service.rpc.server.DimensionConverterServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;


/**
 * Created by Zhou Ning on 2017/11/24.
 * <p>
 * Desc:操作dimension converter相关服务的client端工具类
 */
public class DimensionConverterClient {

    public static IDimensionConverter createDimensionConverter(Configuration conf) throws IOException {
        //创建操作
        String[] cf = fetchDimensionConverterConfiguration(conf);
        String address = cf[0];//获取ip地址
        int port = Integer.valueOf(cf[1]);//获取端口号
        //创建
        return new InnerDimensionConverterProxy(conf, address, port);
    }

    /**
     * 关闭客户端连接
     *
     * @param proxy
     */
    public static void stopDimensionConverterProxy(IDimensionConverter proxy) {
        if (proxy != null) {
            InnerDimensionConverterProxy innerProxy = (InnerDimensionConverterProxy) proxy;
            RPC.stopProxy(innerProxy.proxy);
        }
    }

    /**
     * 读取配置信息
     *
     * @param conf
     * @return
     * @throws IOException
     */
    public static String[] fetchDimensionConverterConfiguration(Configuration conf) throws IOException {
        FileSystem fs = null;
        BufferedReader br = null;
        try {
            fs = FileSystem.get(conf);
            br = new BufferedReader(new InputStreamReader(fs.open(new Path(DimensionConverterServer.CONFIG_SAVE_PATH))));
            String[] result = new String[2];
            result[0] = br.readLine().trim();
            result[1] = br.readLine().trim();
            return result;
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (Exception e) {
                    //nothing
                }
            }
        }
    }

    /**
     * 内部代理类
     */
    private static class InnerDimensionConverterProxy implements IDimensionConverter {

        private IDimensionConverter proxy = null;
        private Map<String, Integer> cache = new LinkedHashMap<String, Integer>() {
            private static final long serialVersionUID = -2588611466067329575L;

            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
                return this.size() > 1000;
            }
        };

        public InnerDimensionConverterProxy(Configuration conf, String address, int port) throws IOException {
            this.proxy = RPC.getProxy(IDimensionConverter.class, IDimensionConverter.versionID, new InetSocketAddress(address, port), conf);
        }

        @Override
        public int getDimensionIdByValue(BaseDimension dimension) throws IOException {
            String key = DimensionConverterImpl.buildCacheKey(dimension);
            Integer value = this.cache.get(key);
            if (value == null) {
                //通知proxy获取数据
                value = this.proxy.getDimensionIdByValue(dimension);
                this.cache.put(key, value);
            }
            return value;
        }

        @Override
        public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
            return this.proxy.getProtocolVersion(protocol, clientVersion);
        }

        @Override
        public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
            return this.proxy.getProtocolSignature(protocol, clientVersion, clientMethodsHash);
        }
    }


} 