/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

import static java.lang.Math.*;

public class HaversineDistDegreeGroupByFunction extends DoubleFunction implements GroupByFunction, TernaryFunction {

    private final static double EARTH_RADIUS = 6371.088;
    private final Function latDegree;
    private final Function lonDegree;
    private final Function timestamp;
    private int valueIndex;

    public HaversineDistDegreeGroupByFunction(@NotNull Function latDegree, @NotNull Function lonDegree, Function timestamp) {
        this.latDegree = latDegree;
        this.lonDegree = lonDegree;
        this.timestamp = timestamp;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        // first item
        saveFirstItem(mapValue, latDegree.getDouble(record), lonDegree.getDouble(record), timestamp.getTimestamp(record));
        // last item
        saveLastItem(mapValue, latDegree.getDouble(record), lonDegree.getDouble(record), timestamp.getTimestamp(record));
        // result
        saveDistance(mapValue, 0);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        double lat1Degrees = getLastLatitude(mapValue);
        double lon1Degrees = getLastLongitude(mapValue);
        long timestamp1 = getLastTimestamp(mapValue);
        double lat2Degrees = latDegree.getDouble(record);
        double lon2Degrees = lonDegree.getDouble(record);
        long timestamp2 = timestamp.getTimestamp(record);
        if (!Double.isNaN(lat1Degrees) && !Double.isNaN(lon1Degrees) && timestamp1 != Numbers.LONG_NaN) {
            if (!Double.isNaN(lat2Degrees) && !Double.isNaN(lon2Degrees) && timestamp2 != Numbers.LONG_NaN) {
                double currentTotalDistance = getDistance(mapValue);
                double distance = calculateHaversineDistanceFromDegrees(lat1Degrees, lon1Degrees, lat2Degrees, lon2Degrees, currentTotalDistance);
                saveLastItem(mapValue, lat2Degrees, lon2Degrees, timestamp2);
                saveDistance(mapValue, distance);
            }
        } else {
            saveLastItem(mapValue, lat2Degrees, lon2Degrees, timestamp2);
        }
    }

    @Override
    public Function getCenter() {
        return this.lonDegree;
    }

    @Override
    public double getDouble(Record rec) {
        return getDistance(rec);
    }

    @Override
    public Function getLeft() {
        return this.latDegree;
    }

    @Override
    public String getName() {
        return "haversine_dist_deg";
    }

    @Override
    public Function getRight() {
        return this.timestamp;
    }

    @Override
    public void interpolateBoundary(
            MapValue value1,
            MapValue value2,
            long boundaryTimestamp,
            boolean isEndOfBoundary
    ) {
        double distance = calculateHaversineDistance(value1, value2);
        // interpolate
        long ts1 = getLastTimestamp(value1);
        long ts2 = getFirstTimestamp(value2);
        long boundaryLength = isEndOfBoundary ? boundaryTimestamp - ts1 : ts2 - boundaryTimestamp;
        double interpolatedBoundaryDistance = (boundaryLength * distance) / (ts2 - ts1);
        // save
        MapValue result = isEndOfBoundary ? value1 : value2;
        saveDistance(interpolatedBoundaryDistance, result, getDistance(result));
    }

    @Override
    public void interpolateGap(
            MapValue result,
            MapValue value1,
            MapValue value2,
            long gapSize
    ) {
        double distance = calculateHaversineDistance(value1, value2);
        // interpolate
        long ts1 = getLastTimestamp(value1);
        long ts2 = getFirstTimestamp(value2);
        double interpolatedGapDistance = (gapSize * distance) / (ts2 - ts1);
        // save
        saveDistance(result, interpolatedGapDistance);
    }

    @Override
    public boolean isInterpolationSupported() {
        return true;
    }

    @Override
    public boolean isScalar() {
        return false;
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        //first item
        columnTypes.add(ColumnType.DOUBLE);
        columnTypes.add(ColumnType.DOUBLE);
        columnTypes.add(ColumnType.LONG);
        //last item
        columnTypes.add(ColumnType.DOUBLE);
        columnTypes.add(ColumnType.DOUBLE);
        columnTypes.add(ColumnType.LONG);
        //result
        columnTypes.add(ColumnType.DOUBLE);
    }

    @Override
    public void setNull(MapValue mapValue) {
        //set null to first item
        saveFirstItem(mapValue, Double.NaN, Double.NaN, Numbers.LONG_NaN);
        //set null to last item
        saveLastItem(mapValue, Double.NaN, Double.NaN, Numbers.LONG_NaN);
        //
        saveDistance(mapValue, 0.0);
    }

    private double calculateHaversineDistance(MapValue value1, MapValue value2) {
        //value1
        double lat1Degrees = getLastLatitude(value1);
        double lon1Degrees = getLastLongitude(value1);

        //value2
        double lat2Degrees = getFirstLatitude(value2);
        double lon2Degrees = getFirstLongitude(value2);

        return calculateHaversineDistanceFromDegrees(lat1Degrees, lon1Degrees, lat2Degrees, lon2Degrees, 0);
    }

    private double calculateHaversineDistanceFromDegrees(double lat1Degrees, double lon1Degrees, double lat2Degrees, double lon2Degrees, double currentTotalDistance) {
        double lat1 = toRad(lat1Degrees);
        double lon1 = toRad(lon1Degrees);
        double lat2 = toRad(lat2Degrees);
        double lon2 = toRad(lon2Degrees);
        return calculateHaversineDistanceFromRadians(currentTotalDistance, lat1, lon1, lat2, lon2);
    }

    private double calculateHaversineDistanceFromRadians(double currentTotal, double lat1, double lon1, double lat2, double lon2) {
        double halfLatDist = (lat2 - lat1) / 2;
        double halfLonDist = (lon2 - lon1) / 2;
        double a = sin(halfLatDist) * sin(halfLatDist) + cos(lat1) * cos(lat2) * sin(halfLonDist) * sin(halfLonDist);
        double c = 2 * atan2(sqrt(a), sqrt(1 - a));
        currentTotal += EARTH_RADIUS * c;
        return currentTotal;
    }

    private double getDistance(Record result) {
        return result.getDouble(valueIndex + 6);
    }

    private double getFirstLatitude(MapValue value) {
        return value.getDouble(valueIndex);
    }

    private double getFirstLongitude(MapValue value) {
        return value.getDouble(valueIndex + 1);
    }

    private long getFirstTimestamp(MapValue value) {
        return value.getTimestamp(valueIndex + 2);
    }

    private double getLastLatitude(MapValue value1) {
        return value1.getDouble(valueIndex + 3);
    }

    private double getLastLongitude(MapValue mapValue) {
        return mapValue.getDouble(valueIndex + 4);
    }

    private long getLastTimestamp(MapValue value) {
        return value.getTimestamp(valueIndex + 5);
    }

    private void saveDistance(double interpolatedBoundaryDistance, MapValue result, double currentDistance) {
        saveDistance(result, currentDistance + interpolatedBoundaryDistance);
    }

    private void saveDistance(MapValue result, double distance) {
        result.putDouble(this.valueIndex + 6, distance);
    }

    private void saveFirstItem(MapValue mapValue, double lat, double lon, long timestamp) {
        mapValue.putDouble(this.valueIndex, lat);
        mapValue.putDouble(this.valueIndex + 1, lon);
        mapValue.putTimestamp(this.valueIndex + 2, timestamp);
    }

    private void saveLastItem(MapValue mapValue, double lat, double lon, long timestamp) {
        mapValue.putDouble(this.valueIndex + 3, lat);
        mapValue.putDouble(this.valueIndex + 4, lon);
        mapValue.putTimestamp(this.valueIndex + 5, timestamp);
    }

    private double toRad(double deg) {
        return deg * PI / 180;
    }
}
