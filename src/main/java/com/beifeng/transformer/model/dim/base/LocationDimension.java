package com.beifeng.transformer.model.dim.base;

import com.beifeng.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhouning on 2017/11/21.
 * desc:
 */
public class LocationDimension extends BaseDimension {
    private int id;
    private String country;
    private String province;
    private String city;

    public LocationDimension() {
        super();
        this.clean();
    }

    public LocationDimension(int id, String contry, String province, String city) {
        super();
        this.id = id;
        this.country = contry;
        this.province = province;
        this.city = city;
    }

    /**
     * 创建Location dimension类
     *
     * @param contry
     * @param province
     * @param city
     * @return
     */
    public static LocationDimension newInstance(String contry, String province, String city) {
        LocationDimension location = new LocationDimension();
        location.country = contry;
        location.province = province;
        location.city = city;
        return location;
    }

    public static List<LocationDimension> buildList(String contry, String province, String city) {
        List<LocationDimension> list = new ArrayList<>();
        if (StringUtils.isBlank(contry) || GlobalConstants.DEFAULT_VALUE.equals(contry)) {
            //国家名称为空，那么将所有的设置为default value，
            // 或者国家名称为default value，那么也需要将所有设置为default value
            contry = province = city = GlobalConstants.DEFAULT_VALUE;
        }
        if (StringUtils.isBlank(province) || GlobalConstants.DEFAULT_VALUE.equals(province)) {
            //省份名称为空，那么将city 和province设置为default value
            province = city = GlobalConstants.DEFAULT_VALUE;
        }
        if (StringUtils.isBlank(city)) {
            //城市名称为空，设置为default value
            city = GlobalConstants.DEFAULT_VALUE;
        }
        list.add(LocationDimension.newInstance(contry, GlobalConstants.DEFAULT_VALUE, GlobalConstants.DEFAULT_VALUE));
        list.add(LocationDimension.newInstance(contry, province, GlobalConstants.DEFAULT_VALUE));
        list.add(LocationDimension.newInstance(contry, province, city));
        return list;
    }

    public void clean() {
        this.id = 0;
        this.country = "";
        this.province = "";
        this.city = "";
    }

    @Override
    public int compareTo(BaseDimension o) {
        LocationDimension other = (LocationDimension) o;
        int tmp = Integer.compare(this.id, other.id);
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.country.compareTo(other.country);
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.province.compareTo(other.province);
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.city.compareTo(other.city);
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeUTF(this.country);
        out.writeUTF(this.province);
        out.writeUTF(this.city);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.country = in.readUTF();
        this.province = in.readUTF();
        this.city = in.readUTF();
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
}
