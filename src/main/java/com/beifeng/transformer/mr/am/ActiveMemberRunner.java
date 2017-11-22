package com.beifeng.transformer.mr.am;

import com.beifeng.common.EventLogConstants;
import com.beifeng.common.GlobalConstants;
import com.beifeng.transformer.model.dim.StatsUserDimension;
import com.beifeng.transformer.model.value.map.TimeOutputValue;
import com.beifeng.transformer.model.value.reduce.MapWritableValue;
import com.beifeng.transformer.mr.TransformerOutputFormat;
import com.beifeng.transformer.mr.au.ActiveUserRunner;
import com.beifeng.util.TimeUtil;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.util.List;

public class ActiveMemberRunner implements Tool {
    private static Logger logger = Logger.getLogger(ActiveMemberRunner.class);
    private Configuration conf = null;

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new ActiveMemberRunner(), args);
        } catch (Exception e) {
            logger.error("运行active_member任务出现异常", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        //初始化参数
        this.processArgs(conf, args);
//创建job
        Job job = Job.getInstance(conf, "active_member");
        //设置job相关配置参数
        job.setJarByClass(ActiveMemberRunner.class);
        //hbase 输入 mapper参数
        //本地运行
        TableMapReduceUtil.initTableMapperJob(this.initScan(job), ActiveMemberMapper.class, StatsUserDimension.class, TimeOutputValue.class, job, false);

        //设置reducer相关参数
        job.setReducerClass(ActiveMemberReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(MapWritableValue.class);

        //设置output相关参数
        job.setOutputFormatClass(TransformerOutputFormat.class);

        long startTime = System.currentTimeMillis();
        try {
            return job.waitForCompletion(true) ? 0 : -1;
        } finally {
            long endTime = System.currentTimeMillis();
            logger.info("Job<" + job.getJobName() + ">是否执行成功：" + job.isSuccessful() + ";开始时间：" + startTime + ";结束时间：" + endTime + ";用时：" + (endTime - startTime) + "ms");
        }
    }

    private List<Scan> initScan(Job job) {
        Configuration conf = job.getConfiguration();

        //获取运行时间：yyyy-MM-dd
        String date = conf.get(GlobalConstants.RUNNING_DATE_PARAMES);
        long startDate = TimeUtil.parseString2Long(date);
        long endDate = startDate + GlobalConstants.DAY_OF_MILLISECONDS;

        Scan scan = new Scan();
        //定义hbase扫描的开始 rowKey 和结束rowKey
        scan.setStopRow(Bytes.toBytes("" + startDate));
        scan.setStopRow(Bytes.toBytes("" + endDate));

        FilterList filterList = new FilterList();
        //定义mapper中需要获取的列名
        String[] columns = new String[]{
                EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID,//会员id
                EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME,//服务器时间
                EventLogConstants.LOG_COLUMN_NAME_PLATFORM,//平台名称
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME,//浏览器名称
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION,//浏览器版本号
                EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME//添加一个事件名称获取列，在使用singleColumnValueFilter的时候必须指定对应的列是一个返回列
        };
        filterList.addFilter(this.getColumnFilter(columns));

        //只需要 page view事件，所以进行过滤
        filterList.addFilter(new SingleColumnValueFilter(ActiveMemberMapper.family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(EventLogConstants.EventEnum.PAGEVIEW.alias)));
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(EventLogConstants.HBASE_NAME_EVENT_LOGS));
        scan.setFilter(filterList);
        return Lists.newArrayList(scan);
    }

    /**
     * 获取列名过滤的column
     *
     * @param columns
     * @return
     */
    private Filter getColumnFilter(String[] columns) {
        int length = columns.length;
        byte[][] filter = new byte[length][];
        for (int i = 0; i < length; i++) {
            filter[i] = Bytes.toBytes(columns[i]);
        }
        return new MultipleColumnPrefixFilter(filter);
    }

    private void processArgs(Configuration conf, String[] args) {
        String date = null;
        for (int i = 0; i < args.length; i++) {
            if ("-d".equals(args[i])) {
                if (i + 1 < args.length) {
                    date = args[i + 1];
                    break;
                }
            }
        }

        //要求date格式为：yyyy-MM-dd
        if (StringUtils.isBlank(date) || !TimeUtil.isValidateRunningDate(date)) {
            //date是一个无效时间数据
            date = TimeUtil.getYesterday();//默认时间是昨天
        }
        conf.set(GlobalConstants.RUNNING_DATE_PARAMES, date);

    }

    @Override
    public void setConf(Configuration configuration) {
        configuration.addResource("output-collector.xml");
        configuration.addResource("query-mapping.xml");
        configuration.addResource("transformer-env.xml");
        //创建hbase相关的config对象，(包含hbase配置文件)
        //hbase创建config的时候，会将指定参数的configuration所有内容加载到内存中
        this.conf = HBaseConfiguration.create(configuration);
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }
}
