package com.beifeng.transformer.mr;

import com.beifeng.common.EventLogConstants;
import com.beifeng.common.GlobalConstants;
import com.beifeng.util.TimeUtil;
import com.google.common.collect.Lists;
import javafx.scene.control.Tab;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import javax.lang.model.SourceVersion;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

/**
 * Created by zhouning on 2017/11/20.
 * desc:
 */
public class TransformerBaseRunner implements Tool {
    private static final Logger logger = Logger.getLogger(TransformerBaseRunner.class);
    protected Configuration conf = null;
    private String jobName;
    private long startTime;
    private boolean isCallSetUpRunnerMethod = false;
    private Class<?> runnerClass;
    private Class<? extends TableMapper<?, ?>> mapperClass;
    private Class<? extends Reducer<?, ?, ?, ?>> reducerClass;
    private Class<? extends OutputFormat<?, ?>> outputFormatClass;
    private Class<? extends WritableComparable<?>> mapOutputKeyClass;
    private Class<? extends Writable> mapOutputValueClass;
    private Class<?> outputKeyClass;
    private Class<?> outputValueClass;

    /**
     * 设置job参数
     * @param jobName job名称
     * @param runnerClass runner class
     * @param mapperClass mapper class
     * @param reducerClass reducer class
     * @param outputKeyClass 输出key
     * @param outputValueClass 输出value
     */
    public void setupRunner(String jobName, Class<?> runnerClass, Class<? extends TableMapper<?, ?>> mapperClass, Class<? extends Reducer<?, ?, ?, ?>> reducerClass, Class<? extends WritableComparable<?>> outputKeyClass, Class<? extends Writable> outputValueClass) {
        this.setupRunner(jobName,runnerClass,mapperClass,reducerClass,outputKeyClass,outputValueClass,outputKeyClass,outputValueClass,TransformerOutputFormat.class);
    }

    public void setupRunner(String jobName, Class<?> runnerClass, Class<? extends TableMapper<?, ?>> mapperClass, Class<? extends Reducer<?, ?, ?, ?>> reducerClass, Class<? extends WritableComparable<?>> outputKeyClass, Class<? extends Writable> outputValueClass, Class<? extends OutputFormat<?, ?>> outputFormatClass){
        this.setupRunner(jobName,runnerClass,mapperClass,reducerClass,outputKeyClass,outputValueClass,outputKeyClass,outputValueClass,outputFormatClass);

    }

    public void setupRunner(String jobName, Class<?> runnerClass, Class<? extends TableMapper<?, ?>> mapperClass, Class<? extends Reducer<?, ?, ?, ?>> reducerClass, Class<? extends WritableComparable<?>> mapOutputKeyClass, Class<? extends Writable> mapOutputValueClass, Class<? extends WritableComparable<?>> outputKeyClass, Class<? extends Writable> outputValueClass) {
        this.setupRunner(jobName,runnerClass,mapperClass,reducerClass,mapOutputKeyClass,mapOutputValueClass,outputKeyClass,outputValueClass,TransformerOutputFormat.class);

    }

    public void setupRunner(String jobName, Class<?> runnerClass, Class<? extends TableMapper<?, ?>> mapperClass, Class<? extends Reducer<?, ?, ?, ?>> reducerClass, Class<? extends WritableComparable<?>> mapOutputKeyClass, Class<? extends Writable> mapOutputValueClass, Class<? extends WritableComparable<?>> outputKeyClass, Class<? extends Writable> outputValueClass, Class<? extends OutputFormat<?, ?>> outputFormatClass) {

        this.jobName = jobName;
        this.runnerClass = runnerClass;
        this.mapperClass = mapperClass;
        this.reducerClass = reducerClass;
        this.mapOutputKeyClass = mapOutputKeyClass;
        this.mapOutputValueClass = mapOutputValueClass;
        this.outputKeyClass = outputKeyClass;
        this.outputValueClass = outputValueClass;
        this.outputFormatClass = outputFormatClass;
        this.isCallSetUpRunnerMethod = true;

    }


    public void startRunner(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), this, args);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (!this.isCallSetUpRunnerMethod) {
            throw new RuntimeException("必须调用setupRunner方法进行参数设置");
        }
        Configuration conf = this.getConf();
        //初始化参数
        this.processArgs(conf, args);

        Job job = this.initJob(conf);

        //执行Job
        this.beforeRunJob(job);//在job执行前运行
        Throwable error = null;
        try {
            this.startTime = System.currentTimeMillis();
            return job.waitForCompletion(true) ? 0 : -1;
        } catch (Throwable e) {
            error = e;
            logger.error("执行" + this.jobName + " job 出现异常", e);
            throw new RuntimeException(e);
        } finally {
            this.afterRunJob(job, error);//在代码执行后运行
        }
    }

    protected void afterRunJob(Job job, Throwable error) throws IOException{
        try {
            //结束的毫秒数
            long endTime = System.currentTimeMillis();
            logger.info("Job<" + this.jobName + ">是否执行成功：" + (error == null ? job.isSuccessful() : "false") + ";开始时间：" + startTime + ";结束时间：" + endTime
                    + ";用时：" + (endTime - startTime) + "ms" + (error == null ? "" : ";异常信息为：" + error.getMessage()));
        } catch (Exception e) {
            //nothing
        }
    }

    protected void beforeRunJob(Job job) {
        //nothing
    }

    private Job initJob(Configuration conf) throws IOException {
        Job job = Job.getInstance(conf, this.jobName);

        job.setJarByClass(this.runnerClass);
        //本地运行
        TableMapReduceUtil.initTableMapperJob(initScan(job), this.mapperClass, this.mapOutputKeyClass, this.mapOutputValueClass, job, false);
        //集群运行：本地提交和打包(jar)提交
//        TableMapReduceUtil.initTableMapperJob(initScan(job),this.mapperClass,this.mapOutputKeyClass,this.mapOutputValueClass,job);
        job.setReducerClass(this.reducerClass);
        job.setOutputKeyClass(this.outputKeyClass);
        job.setOutputValueClass(this.outputValueClass);

        job.setOutputFormatClass(this.outputFormatClass);
        return job;
    }

    private List<Scan> initScan(Job job) {
        Configuration conf = job.getConfiguration();
        String date = conf.get(GlobalConstants.RUNNING_DATE_PARAMES);
        long startDate = TimeUtil.parseString2Long(date);
        long endDate = startDate + GlobalConstants.DAY_OF_MILLISECONDS;

        Scan scan = new Scan();
        //定义hbase扫描的开始rowKey和结束rowKey
        scan.setStartRow(Bytes.toBytes("" + startDate));
        scan.setStopRow(Bytes.toBytes("" + endDate));

        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(EventLogConstants.HBASE_NAME_EVENT_LOGS));
        Filter filter = this.fetchHbaseFilter();
        if (filter != null) {
            scan.setFilter(filter);
        }
        return Lists.newArrayList(scan);

    }

    /**
     * 获取hbase操作的过滤filter对象
     *
     * @return
     */
    protected Filter fetchHbaseFilter() {
        return null;
    }

    /**
     * 获取这个列名过滤的column
     *
     * @param columns
     * @return
     */
    protected Filter getColumnFilter(String[] columns) {

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

        //添加自定义的配置文件
        configuration.addResource("output-collector.xml");
        configuration.addResource("query-mapping.xml");
        configuration.addResource("transformer-env.xml");
        //创建bhase相关的config对象(包含hbase配置文件)
        //hbase创建config的时候，会将指定参数的configuration所有的内容加载到内存中
        this.conf = HBaseConfiguration.create(configuration);
        //调用设置自定义的函数
        this.configure();
    }

    private void configure(String... resourceFiles) {
        if (this.conf == null) {
            this.conf = HBaseConfiguration.create();
        }
        //开始添加指定的资源文件
        if (resourceFiles != null) {
            for (String resource : resourceFiles) {
                this.conf.addResource(resource);
            }
        }
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }
}
