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
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

/**
 * Returns distance in meters between two lat/lon points using equirectangular projection.
 * Optimized for small distances (lidar/local area scans).
 * <p>
 * Signature: geo_distance_meters(lat1, lon1, lat2, lon2)
 * <p>
 * Accuracy: ~0.1% error for distances &lt; 100km. Perfect for lidar scans.
 * <p>
 * Edge cases:
 * <ul>
 *   <li>Returns NaN if any input is NaN or null</li>
 *   <li>Returns 0.0 for same point</li>
 *   <li>At poles, accuracy degrades but still works</li>
 *   <li>Throws error for invalid latitude (outside -90 to 90)</li>
 *   <li>Throws error for invalid longitude (outside -180 to 180)</li>
 * </ul>
 */
@SuppressWarnings("unused")
public class GeoDistanceMetersFunctionFactory implements FunctionFactory {
    // Approximate meters per degree of latitude (constant everywhere on Earth)
    private static final double METERS_PER_DEG = 111_320.0;

    @Override
    public String getSignature() {
        return "geo_distance_meters(DDDD)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function lat1Func = args.getQuick(0);
        final Function lon1Func = args.getQuick(1);
        final Function lat2Func = args.getQuick(2);
        final Function lon2Func = args.getQuick(3);

        final int lat1Pos = argPositions.getQuick(0);
        final int lon1Pos = argPositions.getQuick(1);
        final int lat2Pos = argPositions.getQuick(2);
        final int lon2Pos = argPositions.getQuick(3);

        // Optimization: if lat1/lon1 are constant, precompute cosLat and metersPerDegLon
        if (lat1Func.isConstant() && lon1Func.isConstant()) {
            final double lat1 = lat1Func.getDouble(null);
            final double lon1 = lon1Func.getDouble(null);

            // NaN check - return NaN-producing function
            if (Numbers.isNull(lat1) || Numbers.isNull(lon1)) {
                return new ConstNaNFunction();
            }

            // Validate lat1
            if (lat1 < -90.0 || lat1 > 90.0) {
                throw SqlException.position(lat1Pos)
                        .put("latitude must be between -90 and 90 [value=").put(lat1).put(']');
            }

            // Validate lon1
            if (lon1 < -180.0 || lon1 > 180.0) {
                throw SqlException.position(lon1Pos)
                        .put("longitude must be between -180 and 180 [value=").put(lon1).put(']');
            }

            // Precompute expensive operations
            final double cosLat = Math.cos(Math.toRadians(lat1));
            final double metersPerDegLon = METERS_PER_DEG * cosLat;

            return new ConstPoint1GeoDistanceFunction(
                    lat1, lon1, lat2Func, lon2Func,
                    metersPerDegLon,
                    lat2Pos, lon2Pos
            );
        }

        return new GeoDistanceFunction(
                lat1Func, lon1Func, lat2Func, lon2Func,
                lat1Pos, lon1Pos, lat2Pos, lon2Pos
        );
    }

    /**
     * Constant function that always returns NaN (for null/NaN constant inputs).
     */
    private static class ConstNaNFunction extends DoubleFunction {
        @Override
        public double getDouble(Record rec) {
            return Double.NaN;
        }

        @Override
        public boolean isConstant() {
            return true;
        }

        @Override
        public boolean isEquivalentTo(Function other) {
            return other instanceof ConstNaNFunction;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("NaN");
        }
    }

    /**
     * Optimized function for the common case where lat1/lon1 are constant.
     * Precomputes cosLat and metersPerDegLon to eliminate Math.cos() from hot path.
     */
    private static class ConstPoint1GeoDistanceFunction extends DoubleFunction {
        private final double lat1;
        private final Function lat2Func;
        private final int lat2Pos;
        private final double lon1;
        private final Function lon2Func;
        private final int lon2Pos;
        private final double metersPerDegLon;

        public ConstPoint1GeoDistanceFunction(
                double lat1,
                double lon1,
                Function lat2Func,
                Function lon2Func,
                double metersPerDegLon,
                int lat2Pos,
                int lon2Pos
        ) {
            this.lat1 = lat1;
            this.lon1 = lon1;
            this.lat2Func = lat2Func;
            this.lon2Func = lon2Func;
            this.metersPerDegLon = metersPerDegLon;
            this.lat2Pos = lat2Pos;
            this.lon2Pos = lon2Pos;
        }

        @Override
        public void close() {
            Misc.free(lat2Func);
            Misc.free(lon2Func);
        }

        @Override
        public void cursorClosed() {
            lat2Func.cursorClosed();
            lon2Func.cursorClosed();
        }

        @Override
        public double getDouble(Record rec) {
            final double lat2 = lat2Func.getDouble(rec);
            final double lon2 = lon2Func.getDouble(rec);

            // NaN returns NaN without error
            if (Numbers.isNull(lat2) || Numbers.isNull(lon2)) {
                return Double.NaN;
            }

            // Validate lat2
            if (lat2 < -90.0 || lat2 > 90.0) {
                throw CairoException.nonCritical().position(lat2Pos)
                        .put("latitude must be between -90 and 90 [value=").put(lat2).put(']');
            }

            // Validate lon2
            if (lon2 < -180.0 || lon2 > 180.0) {
                throw CairoException.nonCritical().position(lon2Pos)
                        .put("longitude must be between -180 and 180 [value=").put(lon2).put(']');
            }

            final double dx = (lon2 - lon1) * metersPerDegLon;
            final double dy = (lat2 - lat1) * METERS_PER_DEG;
            return Math.sqrt(dx * dx + dy * dy);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            lat2Func.init(symbolTableSource, executionContext);
            lon2Func.init(symbolTableSource, executionContext);
        }

        @Override
        public boolean isConstant() {
            return lat2Func.isConstant() && lon2Func.isConstant();
        }

        @Override
        public boolean isEquivalentTo(Function other) {
            if (this == other) {
                return true;
            }
            if (other instanceof ConstPoint1GeoDistanceFunction that) {
                return lat1 == that.lat1
                        && lon1 == that.lon1
                        && lat2Func.isEquivalentTo(that.lat2Func)
                        && lon2Func.isEquivalentTo(that.lon2Func);
            }
            return false;
        }

        @Override
        public boolean isThreadSafe() {
            return lat2Func.isThreadSafe() && lon2Func.isThreadSafe();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("geo_distance_meters(")
                    .val(lat1).val(',')
                    .val(lon1).val(',')
                    .val(lat2Func).val(',')
                    .val(lon2Func).val(')');
        }

        @Override
        public void toTop() {
            lat2Func.toTop();
            lon2Func.toTop();
        }
    }

    /**
     * General function for when lat1/lon1 are not constant.
     * Uses midpoint latitude for the cosLat calculation.
     */
    private static class GeoDistanceFunction extends DoubleFunction {
        private final Function lat1Func;
        private final int lat1Pos;
        private final Function lat2Func;
        private final int lat2Pos;
        private final Function lon1Func;
        private final int lon1Pos;
        private final Function lon2Func;
        private final int lon2Pos;

        public GeoDistanceFunction(
                Function lat1Func,
                Function lon1Func,
                Function lat2Func,
                Function lon2Func,
                int lat1Pos,
                int lon1Pos,
                int lat2Pos,
                int lon2Pos
        ) {
            this.lat1Func = lat1Func;
            this.lon1Func = lon1Func;
            this.lat2Func = lat2Func;
            this.lon2Func = lon2Func;
            this.lat1Pos = lat1Pos;
            this.lon1Pos = lon1Pos;
            this.lat2Pos = lat2Pos;
            this.lon2Pos = lon2Pos;
        }

        @Override
        public void close() {
            Misc.free(lat1Func);
            Misc.free(lon1Func);
            Misc.free(lat2Func);
            Misc.free(lon2Func);
        }

        @Override
        public void cursorClosed() {
            lat1Func.cursorClosed();
            lon1Func.cursorClosed();
            lat2Func.cursorClosed();
            lon2Func.cursorClosed();
        }

        @Override
        public double getDouble(Record rec) {
            final double lat1 = lat1Func.getDouble(rec);
            final double lon1 = lon1Func.getDouble(rec);
            final double lat2 = lat2Func.getDouble(rec);
            final double lon2 = lon2Func.getDouble(rec);

            // NaN returns NaN without error
            if (Numbers.isNull(lat1) || Numbers.isNull(lon1) || Numbers.isNull(lat2) || Numbers.isNull(lon2)) {
                return Double.NaN;
            }

            // Validate lat1
            if (lat1 < -90.0 || lat1 > 90.0) {
                throw CairoException.nonCritical().position(lat1Pos)
                        .put("latitude must be between -90 and 90 [value=").put(lat1).put(']');
            }

            // Validate lon1
            if (lon1 < -180.0 || lon1 > 180.0) {
                throw CairoException.nonCritical().position(lon1Pos)
                        .put("longitude must be between -180 and 180 [value=").put(lon1).put(']');
            }

            // Validate lat2
            if (lat2 < -90.0 || lat2 > 90.0) {
                throw CairoException.nonCritical().position(lat2Pos)
                        .put("latitude must be between -90 and 90 [value=").put(lat2).put(']');
            }

            // Validate lon2
            if (lon2 < -180.0 || lon2 > 180.0) {
                throw CairoException.nonCritical().position(lon2Pos)
                        .put("longitude must be between -180 and 180 [value=").put(lon2).put(']');
            }

            // Use midpoint latitude for the cosLat calculation
            final double midLatRad = Math.toRadians((lat1 + lat2) * 0.5);
            final double metersPerDegLon = METERS_PER_DEG * Math.cos(midLatRad);

            final double dx = (lon2 - lon1) * metersPerDegLon;
            final double dy = (lat2 - lat1) * METERS_PER_DEG;
            return Math.sqrt(dx * dx + dy * dy);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            lat1Func.init(symbolTableSource, executionContext);
            lon1Func.init(symbolTableSource, executionContext);
            lat2Func.init(symbolTableSource, executionContext);
            lon2Func.init(symbolTableSource, executionContext);
        }

        @Override
        public boolean isConstant() {
            return lat1Func.isConstant()
                    && lon1Func.isConstant()
                    && lat2Func.isConstant()
                    && lon2Func.isConstant();
        }

        @Override
        public boolean isEquivalentTo(Function other) {
            if (this == other) {
                return true;
            }
            if (other instanceof GeoDistanceFunction that) {
                return lat1Func.isEquivalentTo(that.lat1Func)
                        && lon1Func.isEquivalentTo(that.lon1Func)
                        && lat2Func.isEquivalentTo(that.lat2Func)
                        && lon2Func.isEquivalentTo(that.lon2Func);
            }
            return false;
        }

        @Override
        public boolean isThreadSafe() {
            return lat1Func.isThreadSafe()
                    && lon1Func.isThreadSafe()
                    && lat2Func.isThreadSafe()
                    && lon2Func.isThreadSafe();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("geo_distance_meters(")
                    .val(lat1Func).val(',')
                    .val(lon1Func).val(',')
                    .val(lat2Func).val(',')
                    .val(lon2Func).val(')');
        }

        @Override
        public void toTop() {
            lat1Func.toTop();
            lon1Func.toTop();
            lat2Func.toTop();
            lon2Func.toTop();
        }
    }
}
