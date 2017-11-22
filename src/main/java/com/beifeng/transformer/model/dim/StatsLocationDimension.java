package com.beifeng.transformer.model.dim;

import com.beifeng.transformer.model.dim.base.BaseDimension;
import com.beifeng.transformer.model.dim.base.LocationDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zhouning on 2017/11/21.
 * desc:
 */
public class StatsLocationDimension extends StatsDimension {
    private StatsCommonDimension statsCommon = new StatsCommonDimension();
    private LocationDimension location = new LocationDimension();

    public StatsLocationDimension() {
    }

    public StatsLocationDimension(StatsCommonDimension statsCommon, LocationDimension location) {
        this.statsCommon = statsCommon;
        this.location = location;
    }

    /**
     * 根据现有的location对象复制一个
     *
     * @param dimension
     * @return
     */
    public static StatsLocationDimension clone(StatsLocationDimension dimension) {
        StatsLocationDimension newDimension = new StatsLocationDimension();
        newDimension.statsCommon = StatsCommonDimension.clone(dimension.statsCommon);
        newDimension.location = LocationDimension.newInstance(dimension.location.getCountry(), dimension.location.getProvince(), dimension.location.getCity());
        newDimension.location.setId(dimension.location.getId());
        return newDimension;
    }

    @Override
    public int compareTo(BaseDimension o) {

        StatsLocationDimension other = (StatsLocationDimension) o;
        int tmp = this.statsCommon.compareTo(other.statsCommon);
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.location.compareTo(other.location);
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.statsCommon.write(out);
        this.location.write(out);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.statsCommon.readFields(dataInput);
        this.location.readFields(dataInput);
    }

    public StatsCommonDimension getStatsCommon() {
        return statsCommon;
    }

    public void setStatsCommon(StatsCommonDimension statsCommon) {
        this.statsCommon = statsCommon;
    }

    public LocationDimension getLocation() {
        return location;
    }

    public void setLocation(LocationDimension location) {
        this.location = location;
    }
}
