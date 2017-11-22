package com.beifeng.transformer.util;

/**
 * Created by zhouning on 2017/11/17.
 * desc:用于计算会话长度的类，用于计算一段时间节点之间的长度
 */
public class TimeChain {

    //存储数据的长度
    private static final int CHAINS_SIZE = 2;
    //存储的数据
    private long[] times;
    private int index;//当前下标
    private int size;//当前数据个数
    private int tmpTime;

    public TimeChain(long time) {
        this.times = new long[CHAINS_SIZE];
        this.times[0] = time;
        this.index = 0;
        this.size = 1;
        this.tmpTime = 0;
    }

    /**
     * 添加时间，只保存最小和最大时间
     *
     * @param time
     */
    public void addTime(long time) {
        if (this.size == 1) {
            //表示此时只保存了一个数据
            long temp = this.times[this.index];
            if (temp > time) {
                //要加入的时间小于存在的时间
                this.times[this.index] = time;
                this.times[1] = temp;
            } else {
                //要加入的时间大于存在的时间
                this.times[1] = time;
            }
            this.index = 1;
            this.size = 2;
        } else if (this.size == 2) {
            //表示此时已保存了两个时间值
            long first = this.times[0];
            long second = this.times[1];
            if (time < first) {
                //要加入的时间比最小时间还要小
                this.times[0] = time;
            }
            if (time > second) {
                //要加入的时间比最大时间还要大
                this.times[1] = time;
            }
        }
    }

    /**
     * 获取最小时间戳
     *
     * @return
     */
    public long getMinTime() {
        return this.times[0];
    }

    /**
     * 获取最大时间戳
     *
     * @return
     */
    public long getMaxTime() {
        return this.times[this.size - 1];
    }

    /**
     * 获取时间间隔(毫秒数)
     *
     * @return
     */
    public long getTimeOfMillis() {
        return this.getMaxTime() - this.getMinTime();
    }

    /**
     * 获取时间间隔，秒
     *
     * @return
     */
    public int getTimeOfSecond() {
        return (int) (this.getTimeOfMillis() / 1000);
    }

    public int getTmpTime() {
        return tmpTime;
    }

    public void setTmpTime(int tmpTime) {
        this.tmpTime = tmpTime;
    }


}
