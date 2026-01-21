/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.griffin.engine.functions.geo;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

/**
 * Returns true if point (lat, lon) is within radius_meters of (center_lat, center_lon).
 * Uses equirectangular projection for fast local distance calculations.
 * <p>
 * Signature: geo_within_radius_latlon(lat, lon, center_lat, center_lon, radius_meters)
 * <p>
 * Accuracy: Equirectangular approximation is accurate to ~0.1% for distances &lt; 100km.
 * For lidar scans (typically &lt; 1km), error is negligible.
 * <p>
 * Edge cases:
 * <ul>
 *   <li>Returns false if any input is NaN</li>
 *   <li>Returns true if point is exactly on the boundary (inclusive)</li>
 *   <li>Returns false if radius is negative</li>
 *   <li>At poles (lat ±90), cosLat approaches 0 - still works but less accurate</li>
 * </ul>
 */
@SuppressWarnings("unused")
public class GeoWithinRadiusLatLonFunctionFactory implements FunctionFactory {
    // Threshold for NaN detection: values with (bits & 0x7FFFFFFFFFFFFFFF) > this are NaN
    private static final long INF_BITS = 0x7FF0000000000000L;
    // Approximate meters per degree of latitude (constant everywhere on Earth)
    private static final double METERS_PER_DEG_LAT = 111_320.0;

    @Override
    public String getSignature() {
        return "geo_within_radius_latlon(DDDDD)";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function latFunc = args.getQuick(0);
        final Function lonFunc = args.getQuick(1);
        final Function centerLatFunc = args.getQuick(2);
        final Function centerLonFunc = args.getQuick(3);
        final Function radiusFunc = args.getQuick(4);

        final int latPos = argPositions.getQuick(0);
        final int lonPos = argPositions.getQuick(1);
        final int centerLatPos = argPositions.getQuick(2);
        final int centerLonPos = argPositions.getQuick(3);

        // Optimization: if center and radius are constant, precompute cosLat, metersPerDegLon, radiusSq
        if (centerLatFunc.isConstant() && centerLonFunc.isConstant() && radiusFunc.isConstant()) {
            final double centerLat = centerLatFunc.getDouble(null);
            final double centerLon = centerLonFunc.getDouble(null);
            final double radius = radiusFunc.getDouble(null);

            // Check for NaN or negative radius - these return false without error
            if (Numbers.isNull(centerLat) || Numbers.isNull(centerLon) || Numbers.isNull(radius) || radius < 0) {
                return BooleanConstant.FALSE;
            }

            // Validate center latitude
            if (centerLat < -90.0 || centerLat > 90.0) {
                throw SqlException.position(centerLatPos)
                        .put("latitude must be between -90 and 90 [value=").put(centerLat).put(']');
            }

            // Validate center longitude
            if (centerLon < -180.0 || centerLon > 180.0) {
                throw SqlException.position(centerLonPos)
                        .put("longitude must be between -180 and 180 [value=").put(centerLon).put(']');
            }

            // Precompute expensive operations
            final double cosLat = Math.cos(Math.toRadians(centerLat));
            final double metersPerDegLon = METERS_PER_DEG_LAT * cosLat;
            final double radiusSq = radius * radius;

            return new ConstCenterGeoWithinRadiusLatLonFunction(
                    latFunc, lonFunc, centerLat, centerLon, metersPerDegLon, radius, radiusSq,
                    latPos, lonPos
            );
        }

        return new GeoWithinRadiusLatLonFunction(
                latFunc, lonFunc, centerLatFunc, centerLonFunc, radiusFunc,
                latPos, lonPos, centerLatPos, centerLonPos
        );
    }

    /**
     * Branchless check if point (lat, lon) is within radius of (centerLat, centerLon).
     * Uses precomputed metersPerDegLon and radiusSq for efficiency.
     */
    private static boolean isWithinRadius(
            double lat,
            double lon,
            double centerLat,
            double centerLon,
            double metersPerDegLon,
            double radiusSq
    ) {
        final double dx = (lon - centerLon) * metersPerDegLon;
        final double dy = (lat - centerLat) * METERS_PER_DEG_LAT;
        final double diff = radiusSq - (dx * dx + dy * dy);

        // Convert to raw bits for branchless comparison
        final long bits = Double.doubleToRawLongBits(diff);

        // NaN check: A value is NaN iff (bits & 0x7FFFFFFFFFFFFFFF) > INF_BITS.
        final long nanCheck = INF_BITS - (bits & 0x7FFFFFFFFFFFFFFFL);

        // Point is inside iff diff >= 0 (bits >= 0) and not NaN (nanCheck >= 0).
        return (bits | nanCheck) >= 0;
    }

    /**
     * Optimized function for the common case where center and radius are constant.
     * Precomputes cosLat, metersPerDegLon, and radiusSq to eliminate Math.cos()
     * and Math.toRadians() from the hot path.
     */
    private static class ConstCenterGeoWithinRadiusLatLonFunction extends BooleanFunction {
        private final double centerLat;
        private final double centerLon;
        private final Function latFunc;
        private final int latPos;
        private final Function lonFunc;
        private final int lonPos;
        private final double metersPerDegLon;
        private final double radius;
        private final double radiusSq;

        public ConstCenterGeoWithinRadiusLatLonFunction(
                Function latFunc,
                Function lonFunc,
                double centerLat,
                double centerLon,
                double metersPerDegLon,
                double radius,
                double radiusSq,
                int latPos,
                int lonPos
        ) {
            this.latFunc = latFunc;
            this.lonFunc = lonFunc;
            this.centerLat = centerLat;
            this.centerLon = centerLon;
            this.metersPerDegLon = metersPerDegLon;
            this.radius = radius;
            this.radiusSq = radiusSq;
            this.latPos = latPos;
            this.lonPos = lonPos;
        }

        @Override
        public void close() {
            Misc.free(latFunc);
            Misc.free(lonFunc);
        }

        @Override
        public void cursorClosed() {
            latFunc.cursorClosed();
            lonFunc.cursorClosed();
        }

        @Override
        public boolean getBool(Record rec) {
            final double lat = latFunc.getDouble(rec);
            final double lon = lonFunc.getDouble(rec);

            // NaN returns false without error
            if (Numbers.isNull(lat) || Numbers.isNull(lon)) {
                return false;
            }

            // Validate latitude
            if (lat < -90.0 || lat > 90.0) {
                throw CairoException.nonCritical().position(latPos)
                        .put("latitude must be between -90 and 90 [value=").put(lat).put(']');
            }

            // Validate longitude
            if (lon < -180.0 || lon > 180.0) {
                throw CairoException.nonCritical().position(lonPos)
                        .put("longitude must be between -180 and 180 [value=").put(lon).put(']');
            }

            return isWithinRadius(lat, lon, centerLat, centerLon, metersPerDegLon, radiusSq);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            latFunc.init(symbolTableSource, executionContext);
            lonFunc.init(symbolTableSource, executionContext);
        }

        @Override
        public boolean isConstant() {
            return latFunc.isConstant() && lonFunc.isConstant();
        }

        @Override
        public boolean isEquivalentTo(Function other) {
            if (this == other) {
                return true;
            }
            if (other instanceof ConstCenterGeoWithinRadiusLatLonFunction that) {
                return centerLat == that.centerLat
                        && centerLon == that.centerLon
                        && radius == that.radius
                        && latFunc.isEquivalentTo(that.latFunc)
                        && lonFunc.isEquivalentTo(that.lonFunc);
            }
            return false;
        }

        @Override
        public boolean isNonDeterministic() {
            return latFunc.isNonDeterministic() || lonFunc.isNonDeterministic();
        }

        @Override
        public boolean isRandom() {
            return latFunc.isRandom() || lonFunc.isRandom();
        }

        @Override
        public boolean isRuntimeConstant() {
            return (latFunc.isConstant() || latFunc.isRuntimeConstant())
                    && (lonFunc.isConstant() || lonFunc.isRuntimeConstant());
        }

        @Override
        public boolean isThreadSafe() {
            return latFunc.isThreadSafe() && lonFunc.isThreadSafe();
        }

        @Override
        public boolean shouldMemoize() {
            return latFunc.shouldMemoize() || lonFunc.shouldMemoize();
        }

        @Override
        public boolean supportsParallelism() {
            return latFunc.supportsParallelism() && lonFunc.supportsParallelism();
        }

        @Override
        public boolean supportsRandomAccess() {
            return latFunc.supportsRandomAccess() && lonFunc.supportsRandomAccess();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("geo_within_radius_latlon(")
                    .val(latFunc).val(',')
                    .val(lonFunc).val(',')
                    .val(centerLat).val(',')
                    .val(centerLon).val(',')
                    .val(radius).val(')');
        }

        @Override
        public void toTop() {
            latFunc.toTop();
            lonFunc.toTop();
        }
    }

    private static class GeoWithinRadiusLatLonFunction extends BooleanFunction {
        private final Function centerLatFunc;
        private final int centerLatPos;
        private final Function centerLonFunc;
        private final int centerLonPos;
        private final Function latFunc;
        private final int latPos;
        private final Function lonFunc;
        private final int lonPos;
        private final Function radiusFunc;

        public GeoWithinRadiusLatLonFunction(
                Function latFunc,
                Function lonFunc,
                Function centerLatFunc,
                Function centerLonFunc,
                Function radiusFunc,
                int latPos,
                int lonPos,
                int centerLatPos,
                int centerLonPos
        ) {
            this.latFunc = latFunc;
            this.lonFunc = lonFunc;
            this.centerLatFunc = centerLatFunc;
            this.centerLonFunc = centerLonFunc;
            this.radiusFunc = radiusFunc;
            this.latPos = latPos;
            this.lonPos = lonPos;
            this.centerLatPos = centerLatPos;
            this.centerLonPos = centerLonPos;
        }

        @Override
        public void close() {
            Misc.free(latFunc);
            Misc.free(lonFunc);
            Misc.free(centerLatFunc);
            Misc.free(centerLonFunc);
            Misc.free(radiusFunc);
        }

        @Override
        public void cursorClosed() {
            latFunc.cursorClosed();
            lonFunc.cursorClosed();
            centerLatFunc.cursorClosed();
            centerLonFunc.cursorClosed();
            radiusFunc.cursorClosed();
        }

        @Override
        public boolean getBool(Record rec) {
            final double radius = radiusFunc.getDouble(rec);
            // NaN or negative radius returns false without error
            if (Numbers.isNull(radius) || radius < 0) {
                return false;
            }

            final double centerLat = centerLatFunc.getDouble(rec);
            final double centerLon = centerLonFunc.getDouble(rec);
            final double lat = latFunc.getDouble(rec);
            final double lon = lonFunc.getDouble(rec);

            // NaN returns false without error
            if (Numbers.isNull(centerLat) || Numbers.isNull(centerLon) || Numbers.isNull(lat) || Numbers.isNull(lon)) {
                return false;
            }

            // Validate center latitude
            if (centerLat < -90.0 || centerLat > 90.0) {
                throw CairoException.nonCritical().position(centerLatPos)
                        .put("latitude must be between -90 and 90 [value=").put(centerLat).put(']');
            }

            // Validate center longitude
            if (centerLon < -180.0 || centerLon > 180.0) {
                throw CairoException.nonCritical().position(centerLonPos)
                        .put("longitude must be between -180 and 180 [value=").put(centerLon).put(']');
            }

            // Validate point latitude
            if (lat < -90.0 || lat > 90.0) {
                throw CairoException.nonCritical().position(latPos)
                        .put("latitude must be between -90 and 90 [value=").put(lat).put(']');
            }

            // Validate point longitude
            if (lon < -180.0 || lon > 180.0) {
                throw CairoException.nonCritical().position(lonPos)
                        .put("longitude must be between -180 and 180 [value=").put(lon).put(']');
            }

            final double cosLat = Math.cos(Math.toRadians(centerLat));
            final double metersPerDegLon = METERS_PER_DEG_LAT * cosLat;

            return isWithinRadius(lat, lon, centerLat, centerLon, metersPerDegLon, radius * radius);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            latFunc.init(symbolTableSource, executionContext);
            lonFunc.init(symbolTableSource, executionContext);
            centerLatFunc.init(symbolTableSource, executionContext);
            centerLonFunc.init(symbolTableSource, executionContext);
            radiusFunc.init(symbolTableSource, executionContext);
        }

        @Override
        public boolean isConstant() {
            return latFunc.isConstant()
                    && lonFunc.isConstant()
                    && centerLatFunc.isConstant()
                    && centerLonFunc.isConstant()
                    && radiusFunc.isConstant();
        }

        @Override
        public boolean isEquivalentTo(Function other) {
            if (this == other) {
                return true;
            }
            if (other instanceof GeoWithinRadiusLatLonFunction that) {
                return latFunc.isEquivalentTo(that.latFunc)
                        && lonFunc.isEquivalentTo(that.lonFunc)
                        && centerLatFunc.isEquivalentTo(that.centerLatFunc)
                        && centerLonFunc.isEquivalentTo(that.centerLonFunc)
                        && radiusFunc.isEquivalentTo(that.radiusFunc);
            }
            return false;
        }

        @Override
        public boolean isNonDeterministic() {
            return latFunc.isNonDeterministic()
                    || lonFunc.isNonDeterministic()
                    || centerLatFunc.isNonDeterministic()
                    || centerLonFunc.isNonDeterministic()
                    || radiusFunc.isNonDeterministic();
        }

        @Override
        public boolean isRandom() {
            return latFunc.isRandom()
                    || lonFunc.isRandom()
                    || centerLatFunc.isRandom()
                    || centerLonFunc.isRandom()
                    || radiusFunc.isRandom();
        }

        @Override
        public boolean isRuntimeConstant() {
            return (latFunc.isConstant() || latFunc.isRuntimeConstant())
                    && (lonFunc.isConstant() || lonFunc.isRuntimeConstant())
                    && (centerLatFunc.isConstant() || centerLatFunc.isRuntimeConstant())
                    && (centerLonFunc.isConstant() || centerLonFunc.isRuntimeConstant())
                    && (radiusFunc.isConstant() || radiusFunc.isRuntimeConstant());
        }

        @Override
        public boolean isThreadSafe() {
            return latFunc.isThreadSafe()
                    && lonFunc.isThreadSafe()
                    && centerLatFunc.isThreadSafe()
                    && centerLonFunc.isThreadSafe()
                    && radiusFunc.isThreadSafe();
        }

        @Override
        public boolean shouldMemoize() {
            return latFunc.shouldMemoize()
                    || lonFunc.shouldMemoize()
                    || centerLatFunc.shouldMemoize()
                    || centerLonFunc.shouldMemoize()
                    || radiusFunc.shouldMemoize();
        }

        @Override
        public boolean supportsParallelism() {
            return latFunc.supportsParallelism()
                    && lonFunc.supportsParallelism()
                    && centerLatFunc.supportsParallelism()
                    && centerLonFunc.supportsParallelism()
                    && radiusFunc.supportsParallelism();
        }

        @Override
        public boolean supportsRandomAccess() {
            return latFunc.supportsRandomAccess()
                    && lonFunc.supportsRandomAccess()
                    && centerLatFunc.supportsRandomAccess()
                    && centerLonFunc.supportsRandomAccess()
                    && radiusFunc.supportsRandomAccess();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("geo_within_radius_latlon(")
                    .val(latFunc).val(',')
                    .val(lonFunc).val(',')
                    .val(centerLatFunc).val(',')
                    .val(centerLonFunc).val(',')
                    .val(radiusFunc).val(')');
        }

        @Override
        public void toTop() {
            latFunc.toTop();
            lonFunc.toTop();
            centerLatFunc.toTop();
            centerLonFunc.toTop();
            radiusFunc.toTop();
        }
    }
}
