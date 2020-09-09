/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import org.jetbrains.annotations.NotNull;

import static java.lang.Math.*;

public class HaversineDistDegreeGroupByFunction extends DoubleFunction implements GroupByFunction, BinaryFunction {

    private final static double EARTH_RADIUS = 6371.088;
    private final Function latDegree;
    private final Function lonDegree;
    private int valueIndex;

    public HaversineDistDegreeGroupByFunction(int position, @NotNull Function latDegree, @NotNull Function lonDegree) {
        super(position);
        this.latDegree = latDegree;
        this.lonDegree = lonDegree;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        mapValue.putDouble(this.valueIndex, this.latDegree.getDouble(record));
        mapValue.putDouble(this.valueIndex + 1, this.lonDegree.getDouble(record));
        mapValue.putDouble(this.valueIndex + 2, 0);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        double lat1 = toRad(mapValue.getDouble(valueIndex));
        double lon1 = toRad(mapValue.getDouble(valueIndex + 1));
        double lat2Degrees = this.latDegree.getDouble(record);
        double lon2Degrees = this.lonDegree.getDouble(record);
        double lat2 = toRad(lat2Degrees);
        double lon2 = toRad(lon2Degrees);
        if (!Double.isNaN(lat1) && !Double.isNaN(lon1)) {
            if (!Double.isNaN(lat2) && !Double.isNaN(lon2)) {
                double halfLatDist = (lat2 - lat1) / 2;
                double halfLonDist = (lon2 - lon1) / 2;
                double a = sin(halfLatDist) * sin(halfLatDist) + cos(lat1) * cos(lat2) * sin(halfLonDist) * sin(halfLonDist);
                double c = 2 * atan2(sqrt(a), sqrt(1 - a));
                double distance = mapValue.getDouble(this.valueIndex + 2);
                distance += EARTH_RADIUS * c;
                mapValue.putDouble(this.valueIndex, lat2Degrees);
                mapValue.putDouble(this.valueIndex + 1, lon2Degrees);
                mapValue.putDouble(this.valueIndex + 2, distance);
            }
        } else {
            mapValue.putDouble(this.valueIndex, lat2Degrees);
            mapValue.putDouble(this.valueIndex + 1, lon2Degrees);
        }
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.DOUBLE);
        columnTypes.add(ColumnType.DOUBLE);
        columnTypes.add(ColumnType.DOUBLE);
    }

    @Override
    public void setDouble(MapValue mapValue, double value) {
        mapValue.putDouble(this.valueIndex + 2, value);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putDouble(this.valueIndex, Double.NaN);
        mapValue.putDouble(this.valueIndex + 1, Double.NaN);
        mapValue.putDouble(this.valueIndex + 2, 0.0);
    }

    @Override
    public double getDouble(Record rec) {
        return rec.getDouble(this.valueIndex + 2);
    }

    @Override
    public Function getLeft() {
        return this.latDegree;
    }

    @Override
    public Function getRight() {
        return this.lonDegree;
    }

    private double toRad(double deg) {
        return deg * PI / 180;
    }
}
