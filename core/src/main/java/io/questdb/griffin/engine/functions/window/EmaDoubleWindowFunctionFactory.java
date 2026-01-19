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
 * Exponential Moving Average (EMA) window function.
 * <p>
 * Supports three modes:
 * <ul>
 *   <li>'alpha' - direct smoothing factor (0 &lt; alpha &lt;= 1)</li>
 *   <li>'period' - N-period EMA where alpha = 2 / (N + 1)</li>
 *   <li>time unit ('second', 'minute', 'hour', 'day') - time-weighted decay using alpha = 1 - exp(-Δt / τ)</li>
 * </ul>
 * <p>
 * Usage:
 * <pre>
 * avg(price, 'alpha', 0.2) over (partition by symbol order by timestamp)
 * avg(price, 'period', 10) over (partition by symbol order by timestamp)
 * avg(price, 'minute', 5) over (partition by symbol order by timestamp)
 * </pre>
 * <p>
 * Note: ORDER BY is required and custom framing is not allowed.
 */
public class EmaDoubleWindowFunctionFactory extends AbstractWindowFunctionFactory {

    // Column types for partition-based functions: ema, prevTimestamp, hasValue
    private static final ArrayColumnTypes EMA_COLUMN_TYPES;
    private static final String NAME = "avg";
    private static final String SIGNATURE = NAME + "(DSD)";

    // Mode constants
    private static final int MODE_ALPHA = 0;
    private static final int MODE_PERIOD = 1;
    private static final int MODE_TIME_WEIGHTED = 2;

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

        // EMA requires ORDER BY
        if (!windowContext.isOrdered()) {
            throw SqlException.$(position, "avg() requires ORDER BY");
        }

        // EMA doesn't support custom framing - it always uses all preceding rows
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
                    EMA_COLUMN_TYPES
            );

            if (mode == MODE_TIME_WEIGHTED) {
                return new EmaTimeWeightedOverPartitionFunction(
                        map,
                        partitionByRecord,
                        partitionBySink,
                        valueArg,
                        timestampIndex,
                        tau,
                        kind.toString(),
                        paramValue
                );
            } else {
                return new EmaOverPartitionFunction(
                        map,
                        partitionByRecord,
                        partitionBySink,
                        valueArg,
                        alpha,
                        mode == MODE_ALPHA ? "alpha" : "period",
                        paramValue
                );
            }
        } else {
            // No partition by
            if (mode == MODE_TIME_WEIGHTED) {
                return new EmaTimeWeightedOverUnboundedRowsFrameFunction(
                        valueArg,
                        timestampIndex,
                        tau,
                        kind.toString(),
                        paramValue
                );
            } else {
                return new EmaOverUnboundedRowsFrameFunction(
                        valueArg,
                        alpha,
                        mode == MODE_ALPHA ? "alpha" : "period",
                        paramValue
                );
            }
        }
    }

    /**
     * Parse time unit and return tau in native timestamp precision (micros or nanos).
     */
    private static long parseTimeUnit(CharSequence kind, double value, int position, TimestampDriver driver) throws SqlException {
        long tau;
        if (SqlKeywords.isMicrosecondKeyword(kind) || SqlKeywords.isMicrosecondsKeyword(kind)) {
            tau = driver.fromMicros((long) value);
        } else if (SqlKeywords.isMillisecondKeyword(kind) || SqlKeywords.isMillisecondsKeyword(kind)) {
            tau = driver.fromMillis((long) value);
        } else if (SqlKeywords.isSecondKeyword(kind) || SqlKeywords.isSecondsKeyword(kind)) {
            tau = driver.fromSeconds((long) value);
        } else if (SqlKeywords.isMinuteKeyword(kind) || SqlKeywords.isMinutesKeyword(kind)) {
            tau = driver.fromMinutes((int) value);
        } else if (SqlKeywords.isHourKeyword(kind) || SqlKeywords.isHoursKeyword(kind)) {
            tau = driver.fromHours((int) value);
        } else if (SqlKeywords.isDayKeyword(kind) || SqlKeywords.isDaysKeyword(kind)) {
            tau = driver.fromDays((int) value);
        } else if (SqlKeywords.isWeekKeyword(kind) || SqlKeywords.isWeeksKeyword(kind)) {
            tau = driver.fromWeeks((int) value);
        } else {
            throw SqlException.$(position, "invalid kind parameter: expected 'alpha', 'period', or a time unit (second, minute, hour, day, week)");
        }
        if (tau <= 0) {
            throw SqlException.$(position, "time constant must be at least 1 unit in native timestamp precision");
        }
        return tau;
    }

    // EMA with fixed alpha, with partition by
    static class EmaOverPartitionFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        private final double alpha;
        private final String kindStr;
        private final double paramValue;
        private double ema;

        EmaOverPartitionFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                Function arg,
                double alpha,
                String kindStr,
                double paramValue
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.alpha = alpha;
            this.kindStr = kindStr;
            this.paramValue = paramValue;
        }

        @Override
        public void computeNext(Record record) {
            // map stores:
            // 0 - ema (double)
            // 1 - prevTimestamp (long) - not used for fixed alpha
            // 2 - hasValue (long, 0 or 1)

            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            double d = arg.getDouble(record);

            if (value.isNew()) {
                // First value for this partition
                if (Numbers.isFinite(d)) {
                    value.putDouble(0, d);
                    value.putLong(1, 0);
                    value.putLong(2, 1);
                    this.ema = d;
                } else {
                    value.putDouble(0, Double.NaN);
                    value.putLong(1, 0);
                    value.putLong(2, 0);
                    this.ema = Double.NaN;
                }
            } else {
                long hasValue = value.getLong(2);
                double prevEma = value.getDouble(0);

                if (Numbers.isFinite(d)) {
                    double newEma;
                    if (hasValue == 1 && Numbers.isFinite(prevEma)) {
                        // EMA = alpha * value + (1 - alpha) * prevEMA
                        newEma = alpha * d + (1 - alpha) * prevEma;
                    } else {
                        // First valid value
                        newEma = d;
                    }
                    value.putDouble(0, newEma);
                    value.putLong(2, 1);
                    this.ema = newEma;
                } else {
                    // Null value - keep previous EMA
                    this.ema = hasValue == 1 ? prevEma : Double.NaN;
                }
            }
        }

        @Override
        public double getDouble(Record rec) {
            return ema;
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
            sink.val('(').val(arg).val(", '").val(kindStr).val("', ").val(paramValue).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row)");
        }
    }

    // Time-weighted EMA with partition by
    static class EmaTimeWeightedOverPartitionFunction extends BasePartitionedWindowFunction implements WindowDoubleFunction {

        private final String kindStr;
        private final double paramValue;
        private final long tau;
        private final int timestampIndex;
        private double ema;

        EmaTimeWeightedOverPartitionFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                Function arg,
                int timestampIndex,
                long tau,
                String kindStr,
                double paramValue
        ) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.timestampIndex = timestampIndex;
            this.tau = tau;
            this.kindStr = kindStr;
            this.paramValue = paramValue;
        }

        @Override
        public void computeNext(Record record) {
            // map stores:
            // 0 - ema (double)
            // 1 - prevTimestamp (long)
            // 2 - hasValue (long, 0 or 1)

            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            double d = arg.getDouble(record);
            long timestamp = record.getTimestamp(timestampIndex);

            if (value.isNew()) {
                // First value for this partition
                if (Numbers.isFinite(d)) {
                    value.putDouble(0, d);
                    value.putLong(1, timestamp);
                    value.putLong(2, 1);
                    this.ema = d;
                } else {
                    value.putDouble(0, Double.NaN);
                    value.putLong(1, timestamp);
                    value.putLong(2, 0);
                    this.ema = Double.NaN;
                }
            } else {
                long hasValue = value.getLong(2);
                double prevEma = value.getDouble(0);
                long prevTimestamp = value.getLong(1);

                if (Numbers.isFinite(d)) {
                    double newEma;
                    if (hasValue == 1 && Numbers.isFinite(prevEma)) {
                        // Time-weighted alpha: alpha = 1 - exp(-Δt / τ)
                        long dt = timestamp - prevTimestamp;
                        double alpha;
                        if (dt <= 0) {
                            // Same timestamp or going backwards - use full weight
                            alpha = 1.0;
                        } else {
                            alpha = 1.0 - Math.exp(-(double) dt / tau);
                        }
                        // EMA = alpha * value + (1 - alpha) * prevEMA
                        newEma = alpha * d + (1 - alpha) * prevEma;
                    } else {
                        // First valid value
                        newEma = d;
                    }
                    value.putDouble(0, newEma);
                    value.putLong(1, timestamp);
                    value.putLong(2, 1);
                    this.ema = newEma;
                } else {
                    // Null value - keep previous EMA but update timestamp
                    value.putLong(1, timestamp);
                    this.ema = hasValue == 1 ? prevEma : Double.NaN;
                }
            }
        }

        @Override
        public double getDouble(Record rec) {
            return ema;
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
            sink.val('(').val(arg).val(", '").val(kindStr).val("', ").val(paramValue).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" rows between unbounded preceding and current row)");
        }
    }

    // EMA with fixed alpha, no partition by
    static class EmaOverUnboundedRowsFrameFunction extends BaseWindowFunction implements WindowDoubleFunction {

        private final double alpha;
        private final String kindStr;
        private final double paramValue;
        private double ema = Double.NaN;
        private boolean hasValue = false;

        EmaOverUnboundedRowsFrameFunction(Function arg, double alpha, String kindStr, double paramValue) {
            super(arg);
            this.alpha = alpha;
            this.kindStr = kindStr;
            this.paramValue = paramValue;
        }

        @Override
        public void computeNext(Record record) {
            double d = arg.getDouble(record);

            if (Numbers.isFinite(d)) {
                if (hasValue && Numbers.isFinite(ema)) {
                    // EMA = alpha * value + (1 - alpha) * prevEMA
                    ema = alpha * d + (1 - alpha) * ema;
                } else {
                    // First valid value
                    ema = d;
                    hasValue = true;
                }
            }
            // If d is NaN, keep previous ema
        }

        @Override
        public double getDouble(Record rec) {
            return ema;
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
            ema = Double.NaN;
            hasValue = false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(arg).val(", '").val(kindStr).val("', ").val(paramValue).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            ema = Double.NaN;
            hasValue = false;
        }
    }

    // Time-weighted EMA, no partition by
    static class EmaTimeWeightedOverUnboundedRowsFrameFunction extends BaseWindowFunction implements WindowDoubleFunction {

        private final String kindStr;
        private final double paramValue;
        private final long tau;
        private final int timestampIndex;
        private double ema = Double.NaN;
        private boolean hasValue = false;
        private long prevTimestamp = Long.MIN_VALUE;

        EmaTimeWeightedOverUnboundedRowsFrameFunction(
                Function arg,
                int timestampIndex,
                long tau,
                String kindStr,
                double paramValue
        ) {
            super(arg);
            this.timestampIndex = timestampIndex;
            this.tau = tau;
            this.kindStr = kindStr;
            this.paramValue = paramValue;
        }

        @Override
        public void computeNext(Record record) {
            double d = arg.getDouble(record);
            long timestamp = record.getTimestamp(timestampIndex);

            if (Numbers.isFinite(d)) {
                if (hasValue && Numbers.isFinite(ema)) {
                    // Time-weighted alpha: alpha = 1 - exp(-Δt / τ)
                    long dt = timestamp - prevTimestamp;
                    double alpha;
                    if (dt <= 0) {
                        // Same timestamp or going backwards - use full weight
                        alpha = 1.0;
                    } else {
                        alpha = 1.0 - Math.exp(-(double) dt / tau);
                    }
                    // EMA = alpha * value + (1 - alpha) * prevEMA
                    ema = alpha * d + (1 - alpha) * ema;
                } else {
                    // First valid value
                    ema = d;
                    hasValue = true;
                }
            }
            prevTimestamp = timestamp;
        }

        @Override
        public double getDouble(Record rec) {
            return ema;
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
            ema = Double.NaN;
            hasValue = false;
            prevTimestamp = Long.MIN_VALUE;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val('(').val(arg).val(", '").val(kindStr).val("', ").val(paramValue).val(')');
            sink.val(" over (rows between unbounded preceding and current row)");
        }

        @Override
        public void toTop() {
            super.toTop();
            ema = Double.NaN;
            hasValue = false;
            prevTimestamp = Long.MIN_VALUE;
        }
    }

    static {
        EMA_COLUMN_TYPES = new ArrayColumnTypes();
        EMA_COLUMN_TYPES.add(ColumnType.DOUBLE); // ema
        EMA_COLUMN_TYPES.add(ColumnType.LONG);   // prevTimestamp
        EMA_COLUMN_TYPES.add(ColumnType.LONG);   // hasValue (0 or 1)
    }
}
