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

package io.questdb.griffin.engine.functions.window;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

/**
 * Volume-Weighted Exponential Moving Average (VWEMA) window function.
 * <p>
 * Extends EMA by weighting each value by its associated volume:
 * <ul>
 *   <li>numerator = alpha * value * volume + (1 - alpha) * prevNumerator</li>
 *   <li>denominator = alpha * volume + (1 - alpha) * prevDenominator</li>
 *   <li>VWEMA = numerator / denominator</li>
 * </ul>
 * <p>
 * Supports three modes:
 * <ul>
 *   <li>'alpha' - direct smoothing factor (0 &lt; alpha &lt;= 1)</li>
 *   <li>'period' - N-period EMA where alpha = 2 / (N + 1)</li>
 *   <li>time unit ('second', 'minute', 'hour', 'day') - time-weighted decay using alpha = 1 - exp(-dt / tau)</li>
 * </ul>
 * <p>
 * Usage:
 * <pre>
 * avg(price, 'alpha', 0.2, volume) over (partition by symbol order by timestamp)
 * avg(price, 'period', 10, volume) over (partition by symbol order by timestamp)
 * avg(price, 'minute', 5, volume) over (partition by symbol order by timestamp)
 * </pre>
 * <p>
 * Note: ORDER BY is required and custom framing is not allowed.
 */
public class VwemaDoubleWindowFunctionFactory extends AbstractWindowFunctionFactory {

    // Mode constants
    private static final int MODE_ALPHA = 0;
    private static final int MODE_PERIOD = 1;
    private static final int MODE_TIME_WEIGHTED = 2;
    private static final String NAME = "avg";
    private static final String SIGNATURE = NAME + "(DSDD)";
    // Column types for partition-based functions: numerator, denominator, prevTimestamp, hasValue
    private static final ArrayColumnTypes VWEMA_COLUMN_TYPES;

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        WindowContext windowContext = sqlExecutionContext.getWindowContext();
        windowContext.validate(position, supportNullsDesc());

        // VWEMA requires ORDER BY
        if (!windowContext.isOrdered()) {
            throw SqlException.$(position, "avg() requires ORDER BY");
        }

        // VWEMA doesn't support custom framing - it always uses all preceding rows
        if (!windowContext.isDefaultFrame()) {
            throw SqlException.$(position, "avg() does not support framing; remove ROWS/RANGE clause");
        }

        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        int timestampIndex = windowContext.getTimestampIndex();
        int timestampType = windowContext.getTimestampType();
        TimestampDriver timestampDriver = ColumnType.getTimestampDriver(timestampType);

        // Parse arguments
        Function valueArg = args.get(0);
        Function kindArg = args.get(1);
        Function paramArg = args.get(2);
        Function volumeArg = args.get(3);

        // Kind must be a constant string
        if (!kindArg.isConstant()) {
            throw SqlException.$(argPositions.getQuick(1), "kind parameter must be a constant");
        }

        CharSequence kind = kindArg.getStrA(null);
        if (kind == null) {
            throw SqlException.$(argPositions.getQuick(1), "kind parameter cannot be null");
        }

        // Parameter must be a constant
        if (!paramArg.isConstant()) {
            throw SqlException.$(argPositions.getQuick(2), "parameter value must be a constant");
        }

        double paramValue = paramArg.getDouble(null);
        if (!Numbers.isFinite(paramValue) || paramValue <= 0) {
            throw SqlException.$(argPositions.getQuick(2), "parameter value must be a positive number");
        }

        // Determine mode and calculate alpha/tau
        int mode;
        double alpha = 0;
        long tau = 0;

        if (SqlKeywords.isAlphaKeyword(kind)) {
            mode = MODE_ALPHA;
            if (paramValue > 1) {
                throw SqlException.$(argPositions.getQuick(2), "alpha must be between 0 (exclusive) and 1 (inclusive)");
            }
            alpha = paramValue;
        } else if (SqlKeywords.isPeriodKeyword(kind)) {
            mode = MODE_PERIOD;
            alpha = 2.0 / (paramValue + 1.0);
        } else {
            // Try to parse as time unit
            mode = MODE_TIME_WEIGHTED;
            tau = parseTimeUnit(kind, paramValue, argPositions.getQuick(1), timestampDriver);
        }

        // Create appropriate function based on partitioning
        if (partitionByRecord != null) {
            Map map = MapFactory.createUnorderedMap(
                    configuration,
                    partitionByKeyTypes,
                    VWEMA_COLUMN_TYPES
            );

            if (mode == MODE_TIME_WEIGHTED) {
                return new VwemaTimeWeightedOverPartitionFunction(
                        map,
                        partitionByRecord,
                        partitionBySink,
                        valueArg,
                        volumeArg,
                        timestampIndex,
                        tau,
                        kind,
                        paramValue
                );
            } else {
                return new VwemaOverPartitionFunction(
                        map,
                        partitionByRecord,
                        partitionBySink,
                        valueArg,
                        volumeArg,
                        alpha,
                        mode == MODE_ALPHA ? "alpha" : "period",
                        paramValue
                );
            }
        } else {
            // No partition by
            if (mode == MODE_TIME_WEIGHTED) {
                return new VwemaTimeWeightedOverUnboundedRowsFrameFunction(
                        valueArg,
                        volumeArg,
                        timestampIndex,
                        tau,
                        kind,
                        paramValue
                );
            } else {
                return new VwemaOverUnboundedRowsFrameFunction(
                        valueArg,
                        volumeArg,
                        alpha,
                        mode == MODE_ALPHA ? "alpha" : "period",
                        paramValue
                );
            }
        }
    }

    /**
     * Parse time unit and return tau in native timestamp precision (micros or nanos).
     * Note: Fractional values are truncated to integers for all time units.
     * For example, avg(price, 'hour', 2.7, volume) uses tau for 2 hours, not 2.7 hours.
     */
    private static long parseTimeUnit(CharSequence kind, double value, int position, TimestampDriver driver) throws SqlException {
        if (SqlKeywords.isMicrosecondKeyword(kind) || SqlKeywords.isMicrosecondsKeyword(kind)) {
            return driver.fromMicros((long) value);
        } else if (SqlKeywords.isMillisecondKeyword(kind) || SqlKeywords.isMillisecondsKeyword(kind)) {
            return driver.fromMillis((long) value);
        } else if (SqlKeywords.isSecondKeyword(kind) || SqlKeywords.isSecondsKeyword(kind)) {
            return driver.fromSeconds((long) value);
        } else if (SqlKeywords.isMinuteKeyword(kind) || SqlKeywords.isMinutesKeyword(kind)) {
            return driver.fromMinutes((int) value);
        } else if (SqlKeywords.isHourKeyword(kind) || SqlKeywords.isHoursKeyword(kind)) {
            return driver.fromHours((int) value);
        } else if (SqlKeywords.isDayKeyword(kind) || SqlKeywords.isDaysKeyword(kind)) {
            return driver.fromDays((int) value);
        } else if (SqlKeywords.isWeekKeyword(kind) || SqlKeywords.isWeeksKeyword(kind)) {
            return driver.fromWeeks((int) value);
        } else {
            throw SqlException.$(position, "invalid kind parameter: expected 'alpha', 'period', or a time unit (second, minute, hour, day, week)");
        }
    }

    // VWEMA with fixed alpha, with partition by
    static class VwemaOverPartitionFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        private final double alpha;
        private final String kindStr;
        private final double paramValue;
        private final Function volumeArg;
        private double vwema;

        VwemaOverPartitionFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                Function valueArg,
                Function volumeArg,
                double alpha,
                String kindStr,
                double paramValue
        ) {
            super(map, partitionByRecord, partitionBySink, valueArg);
            this.volumeArg = volumeArg;
            this.alpha = alpha;
            this.kindStr = kindStr;
            this.paramValue = paramValue;
        }

        @Override
        public void close() {
            super.close();
            volumeArg.close();
        }

        @Override
        public void computeNext(Record record) {
            // map stores:
            // 0 - numerator (double)
            // 1 - denominator (double)
            // 2 - prevTimestamp (long) - not used for fixed alpha
            // 3 - hasValue (long, 0 or 1)

            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            double price = arg.getDouble(record);
            double volume = volumeArg.getDouble(record);

            if (mapValue.isNew()) {
                // First value for this partition
                if (Numbers.isFinite(price) && Numbers.isFinite(volume) && volume > 0) {
                    double numerator = price * volume;
                    //noinspection UnnecessaryLocalVariable
                    double denominator = volume;
                    mapValue.putDouble(0, numerator);
                    mapValue.putDouble(1, denominator);
                    mapValue.putLong(2, 0);
                    mapValue.putLong(3, 1);
                    this.vwema = numerator / denominator;
                } else {
                    mapValue.putDouble(0, Double.NaN);
                    mapValue.putDouble(1, Double.NaN);
                    mapValue.putLong(2, 0);
                    mapValue.putLong(3, 0);
                    this.vwema = Double.NaN;
                }
            } else {
                long hasValue = mapValue.getLong(3);
                double prevNumerator = mapValue.getDouble(0);
                double prevDenominator = mapValue.getDouble(1);

                if (Numbers.isFinite(price) && Numbers.isFinite(volume) && volume > 0) {
                    double newNumerator;
                    double newDenominator;
                    // When hasValue == 1, numerator/denominator are guaranteed finite
                    // (set from finite price*volume and updated with finite arithmetic)
                    if (hasValue == 1) {
                        // VWEMA update:
                        // numerator = alpha * price * volume + (1 - alpha) * prevNumerator
                        // denominator = alpha * volume + (1 - alpha) * prevDenominator
                        newNumerator = alpha * price * volume + (1 - alpha) * prevNumerator;
                        newDenominator = alpha * volume + (1 - alpha) * prevDenominator;
                    } else {
                        // First valid value
                        newNumerator = price * volume;
                        newDenominator = volume;
                    }
                    mapValue.putDouble(0, newNumerator);
                    mapValue.putDouble(1, newDenominator);
                    mapValue.putLong(3, 1);
                    this.vwema = newNumerator / newDenominator;
                } else {
                    // Null/invalid value - keep previous VWEMA
                    // When hasValue == 1, denominator is guaranteed finite and positive
                    // (set from volume > 0 and updated with positive arithmetic)
                    this.vwema = hasValue == 1 ? prevNumerator / prevDenominator : Double.NaN;
                }
            }
        }

        @Override
        public double getDouble(Record rec) {
            return vwema;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(", '").val(kindStr).val("', ").val(paramValue).val(", ").val(volumeArg).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row)");
        }
    }

    // VWEMA with fixed alpha, no partition by
    static class VwemaOverUnboundedRowsFrameFunction extends BaseWindowFunction implements WindowDoubleFunction {

        private final double alpha;
        private final String kindStr;
        private final double paramValue;
        private final Function volumeArg;
        private double denominator = Double.NaN;
        private boolean hasValue = false;
        private double numerator = Double.NaN;

        VwemaOverUnboundedRowsFrameFunction(
                Function valueArg,
                Function volumeArg,
                double alpha,
                String kindStr,
                double paramValue
        ) {
            super(valueArg);
            this.volumeArg = volumeArg;
            this.alpha = alpha;
            this.kindStr = kindStr;
            this.paramValue = paramValue;
        }

        @Override
        public void close() {
            super.close();
            volumeArg.close();
        }

        @Override
        public void computeNext(Record record) {
            double price = arg.getDouble(record);
            double volume = volumeArg.getDouble(record);

            if (Numbers.isFinite(price) && Numbers.isFinite(volume) && volume > 0) {
                // When hasValue is true, numerator/denominator are guaranteed finite
                // (set from finite price*volume and updated with finite arithmetic)
                if (hasValue) {
                    // VWEMA update
                    numerator = alpha * price * volume + (1 - alpha) * numerator;
                    denominator = alpha * volume + (1 - alpha) * denominator;
                } else {
                    // First valid value
                    numerator = price * volume;
                    denominator = volume;
                    hasValue = true;
                }
            }
            // If price/volume is invalid, keep previous state
        }

        @Override
        public double getDouble(Record rec) {
            // When hasValue is true, denominator is guaranteed finite and positive
            // (set from volume > 0 and updated with positive arithmetic)
            return hasValue ? numerator / denominator : Double.NaN;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reset() {
            super.reset();
            numerator = Double.NaN;
            denominator = Double.NaN;
            hasValue = false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(arg).val(", '").val(kindStr).val("', ").val(paramValue).val(", ").val(volumeArg).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            numerator = Double.NaN;
            denominator = Double.NaN;
            hasValue = false;
        }
    }

    // Time-weighted VWEMA with partition by
    static class VwemaTimeWeightedOverPartitionFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        private final CharSequence kindStr;
        private final double paramValue;
        private final long tau;
        private final int timestampIndex;
        private final Function volumeArg;
        private double vwema;

        VwemaTimeWeightedOverPartitionFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                Function valueArg,
                Function volumeArg,
                int timestampIndex,
                long tau,
                CharSequence kindStr,
                double paramValue
        ) {
            super(map, partitionByRecord, partitionBySink, valueArg);
            this.volumeArg = volumeArg;
            this.timestampIndex = timestampIndex;
            this.tau = tau;
            this.kindStr = kindStr;
            this.paramValue = paramValue;
        }

        @Override
        public void close() {
            super.close();
            volumeArg.close();
        }

        @Override
        public void computeNext(Record record) {
            // map stores:
            // 0 - numerator (double)
            // 1 - denominator (double)
            // 2 - prevTimestamp (long)
            // 3 - hasValue (long, 0 or 1)

            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            double price = arg.getDouble(record);
            double volume = volumeArg.getDouble(record);
            long timestamp = record.getTimestamp(timestampIndex);

            if (mapValue.isNew()) {
                // First value for this partition
                if (Numbers.isFinite(price) && Numbers.isFinite(volume) && volume > 0) {
                    double numerator = price * volume;
                    //noinspection UnnecessaryLocalVariable
                    double denominator = volume;
                    mapValue.putDouble(0, numerator);
                    mapValue.putDouble(1, denominator);
                    mapValue.putLong(2, timestamp);
                    mapValue.putLong(3, 1);
                    this.vwema = numerator / denominator;
                } else {
                    mapValue.putDouble(0, Double.NaN);
                    mapValue.putDouble(1, Double.NaN);
                    mapValue.putLong(2, timestamp);
                    mapValue.putLong(3, 0);
                    this.vwema = Double.NaN;
                }
            } else {
                long hasValue = mapValue.getLong(3);
                double prevNumerator = mapValue.getDouble(0);
                double prevDenominator = mapValue.getDouble(1);
                long prevTimestamp = mapValue.getLong(2);

                if (Numbers.isFinite(price) && Numbers.isFinite(volume) && volume > 0) {
                    double newNumerator;
                    double newDenominator;
                    // When hasValue == 1, numerator/denominator are guaranteed finite
                    // (set from finite price*volume and updated with finite arithmetic)
                    if (hasValue == 1) {
                        // Time-weighted alpha: alpha = 1 - exp(-dt / tau)
                        long dt = timestamp - prevTimestamp;
                        double alpha;
                        if (dt <= 0) {
                            // Same timestamp or going backwards - use full weight
                            alpha = 1.0;
                        } else {
                            alpha = 1.0 - Math.exp(-(double) dt / tau);
                        }
                        // VWEMA update
                        newNumerator = alpha * price * volume + (1 - alpha) * prevNumerator;
                        newDenominator = alpha * volume + (1 - alpha) * prevDenominator;
                    } else {
                        // First valid value
                        newNumerator = price * volume;
                        newDenominator = volume;
                    }
                    mapValue.putDouble(0, newNumerator);
                    mapValue.putDouble(1, newDenominator);
                    mapValue.putLong(2, timestamp);
                    mapValue.putLong(3, 1);
                    this.vwema = newNumerator / newDenominator;
                } else {
                    // Null/invalid value - keep previous VWEMA but update timestamp
                    mapValue.putLong(2, timestamp);
                    // When hasValue == 1, denominator is guaranteed finite and positive
                    // (set from volume > 0 and updated with positive arithmetic)
                    this.vwema = hasValue == 1 ? prevNumerator / prevDenominator : Double.NaN;
                }
            }
        }

        @Override
        public double getDouble(Record rec) {
            return vwema;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(", '").val(kindStr).val("', ").val(paramValue).val(", ").val(volumeArg).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row)");
        }
    }

    // Time-weighted VWEMA, no partition by
    static class VwemaTimeWeightedOverUnboundedRowsFrameFunction extends BaseWindowFunction implements WindowDoubleFunction {

        private final CharSequence kindStr;
        private final double paramValue;
        private final long tau;
        private final int timestampIndex;
        private final Function volumeArg;
        private double denominator = Double.NaN;
        private boolean hasValue = false;
        private double numerator = Double.NaN;
        private long prevTimestamp = Long.MIN_VALUE;

        VwemaTimeWeightedOverUnboundedRowsFrameFunction(
                Function valueArg,
                Function volumeArg,
                int timestampIndex,
                long tau,
                CharSequence kindStr,
                double paramValue
        ) {
            super(valueArg);
            this.volumeArg = volumeArg;
            this.timestampIndex = timestampIndex;
            this.tau = tau;
            this.kindStr = kindStr;
            this.paramValue = paramValue;
        }

        @Override
        public void close() {
            super.close();
            volumeArg.close();
        }

        @Override
        public void computeNext(Record record) {
            double price = arg.getDouble(record);
            double volume = volumeArg.getDouble(record);
            long timestamp = record.getTimestamp(timestampIndex);

            if (Numbers.isFinite(price) && Numbers.isFinite(volume) && volume > 0) {
                // When hasValue is true, numerator/denominator are guaranteed finite
                // (set from finite price*volume and updated with finite arithmetic)
                if (hasValue) {
                    // Time-weighted alpha: alpha = 1 - exp(-dt / tau)
                    long dt = timestamp - prevTimestamp;
                    double alpha;
                    if (dt <= 0) {
                        // Same timestamp or going backwards - use full weight
                        alpha = 1.0;
                    } else {
                        alpha = 1.0 - Math.exp(-(double) dt / tau);
                    }
                    // VWEMA update
                    numerator = alpha * price * volume + (1 - alpha) * numerator;
                    denominator = alpha * volume + (1 - alpha) * denominator;
                } else {
                    // First valid value
                    numerator = price * volume;
                    denominator = volume;
                    hasValue = true;
                }
            }
            prevTimestamp = timestamp;
        }

        @Override
        public double getDouble(Record rec) {
            // When hasValue is true, denominator is guaranteed finite and positive
            // (set from volume > 0 and updated with positive arithmetic)
            return hasValue ? numerator / denominator : Double.NaN;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reset() {
            super.reset();
            numerator = Double.NaN;
            denominator = Double.NaN;
            hasValue = false;
            prevTimestamp = Long.MIN_VALUE;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(arg).val(", '").val(kindStr).val("', ").val(paramValue).val(", ").val(volumeArg).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            numerator = Double.NaN;
            denominator = Double.NaN;
            hasValue = false;
            prevTimestamp = Long.MIN_VALUE;
        }
    }

    static {
        VWEMA_COLUMN_TYPES = new ArrayColumnTypes();
        VWEMA_COLUMN_TYPES.add(ColumnType.DOUBLE); // numerator
        VWEMA_COLUMN_TYPES.add(ColumnType.DOUBLE); // denominator
        VWEMA_COLUMN_TYPES.add(ColumnType.LONG);   // prevTimestamp
        VWEMA_COLUMN_TYPES.add(ColumnType.LONG);   // hasValue (0 or 1)
    }
}
