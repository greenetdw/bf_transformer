package com.beifeng.transformer.mr.inbound.bounce;

import com.beifeng.transformer.model.dim.StatsCommonDimension;
import com.beifeng.transformer.model.dim.StatsInboundBounceDimension;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by zhouning on 2017/11/22.
 * desc:自定义的二次排序使用到的类
 */
public class InboundBounceSecondSort {
    /**
     * 自定义分组类
     */
    public static class InboundBounceGroupingComparator extends WritableComparator {

        public InboundBounceGroupingComparator() {
            //注册
            super(StatsInboundBounceDimension.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            StatsInboundBounceDimension key1 = (StatsInboundBounceDimension) a;
            StatsInboundBounceDimension key2 = (StatsInboundBounceDimension) b;
            //根据statsCommon进行分组
            return key1.getStatsCommon().compareTo(key2.getStatsCommon());
        }
    }

    /**
     * 自定义reducer分组函数
     */
    public static class InboundBouncePartitioner extends Partitioner<StatsInboundBounceDimension, IntWritable> {

        private HashPartitioner<StatsCommonDimension, IntWritable> partitioner = new HashPartitioner<>();

        @Override
        public int getPartition(StatsInboundBounceDimension dimension, IntWritable intWritable, int numPartitions) {
            return this.partitioner.getPartition(dimension.getStatsCommon(), intWritable, numPartitions);
        }
    }
}
