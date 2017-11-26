package com.beifeng.transformer.model.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zhouning on 2017/11/26.
 * desc:货币类型dimension类
 */
public class CurrencyTypeDimension extends BaseDimension {
    private int id;
    private String currencyName;

    public CurrencyTypeDimension() {
    }

    public CurrencyTypeDimension(String currencyName) {
        this.currencyName = currencyName;
    }

    public CurrencyTypeDimension(int id, String currencyName) {
        this.id = id;
        this.currencyName = currencyName;
    }

    @Override
    public int compareTo(BaseDimension o) {
        CurrencyTypeDimension other = (CurrencyTypeDimension) o;
        int tmp = Integer.compare(this.id, other.id);
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.currencyName.compareTo(other.currencyName);
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeUTF(this.currencyName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.currencyName = in.readUTF();
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCurrencyName() {
        return currencyName;
    }

    public void setCurrencyName(String currencyName) {
        this.currencyName = currencyName;
    }
}
