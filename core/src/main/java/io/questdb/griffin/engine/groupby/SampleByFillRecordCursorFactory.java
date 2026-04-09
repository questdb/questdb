/*+******************************************************************************
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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.std.IntList;
import io.questdb.std.Long256Impl;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

/**
 * Unified fill cursor for SAMPLE BY on the GROUP BY fast path. Two-pass
 * streaming design that handles keyed and non-keyed queries.
 * <p>
 * Pass 1: iterate sorted base cursor, discover all unique key combinations.
 * Pass 2: iterate again, emit data rows + fill rows for missing keys per bucket.
 * <p>
 * Expects sorted input (ORDER BY ts). Reports followedOrderByAdvice=true.
 */
public class SampleByFillRecordCursorFactory extends AbstractRecordCursorFactory {
    public static final int FILL_CONSTANT = -1;
    public static final int FILL_PREV_SELF = -2;

    private final RecordCursorFactory base;
    private final ObjList<Function> constantFills;
    private final SampleByFillCursor cursor;
    private final Function fromFunc;
    private final boolean hasPrevFill;
    private final long samplingInterval;
    private final char samplingIntervalUnit;
    private final int timestampIndex;
    private final int timestampType;
    private final Function toFunc;

    public SampleByFillRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory base,
            Function fromFunc,
            Function toFunc,
            long samplingInterval,
            char samplingIntervalUnit,
            TimestampSampler timestampSampler,
            IntList fillModes,
            ObjList<Function> constantFills,
            int timestampIndex,
            int timestampType,
            boolean hasPrevFill,
            RecordSink recordSink
    ) {
        super(metadata);
        this.base = base;
        this.fromFunc = fromFunc;
        this.toFunc = toFunc;
        this.samplingInterval = samplingInterval;
        this.samplingIntervalUnit = samplingIntervalUnit;
        this.timestampIndex = timestampIndex;
        this.timestampType = timestampType;
        this.constantFills = constantFills;
        this.hasPrevFill = hasPrevFill;
        this.cursor = new SampleByFillCursor(
                configuration, metadata, timestampSampler,
                fromFunc, toFunc, fillModes, constantFills,
                timestampIndex, timestampType, hasPrevFill
        );
    }

    @Override
    public boolean followedOrderByAdvice() {
        return true;
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            cursor.of(baseCursor, executionContext);
            return cursor;
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Sample By Fill");
        TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        if (fromFunc != driver.getTimestampConstantNull() || toFunc != driver.getTimestampConstantNull()) {
            sink.attr("range").val('(').val(fromFunc).val(',').val(toFunc).val(')');
        }
        sink.attr("stride").val('\'').val(samplingInterval).val(samplingIntervalUnit).val('\'');
        if (hasPrevFill) {
            sink.attr("fill").val("prev");
        }
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    @Override
    protected void _close() {
        base.close();
        Misc.free(fromFunc);
        Misc.free(toFunc);
        Misc.freeObjList(constantFills);
    }

    private static class SampleByFillCursor implements NoRandomAccessRecordCursor {
        private final int columnCount;
        private final short[] columnTypes;
        private final ObjList<Function> constantFills;
        private final FillRecord fillRecord = new FillRecord();
        private final FillTimestampConstant fillTimestampFunc;
        private final IntList fillModes;
        private final Function fromFunc;
        private final boolean hasPrevFill;
        private final TimestampDriver timestampDriver;
        private final int timestampIndex;
        private final TimestampSampler timestampSampler;
        private final Function toFunc;
        private RecordCursor baseCursor;
        private Record baseRecord;
        // Per-key prev values: prevValues[keyIndex * columnCount + col]
        // Flat array to avoid 2D allocation
        private long[] prevValues;
        private boolean[] prevInitialized; // per key: has prev been set?
        // Current state
        private boolean isInitialized;
        private long maxTimestamp;
        private long nextBucketTimestamp;
        // Per-bucket key tracking
        private boolean hasDataForCurrentBucket;
        private boolean emittingFills; // true when emitting fill rows for missing keys
        // For non-keyed: simple flag
        private boolean isNonKeyed;
        // Simple prev for non-keyed
        private long[] simplePrev;
        private boolean hasSimplePrev;

        private SampleByFillCursor(
                CairoConfiguration configuration,
                RecordMetadata metadata,
                TimestampSampler timestampSampler,
                @NotNull Function fromFunc,
                @NotNull Function toFunc,
                IntList fillModes,
                ObjList<Function> constantFills,
                int timestampIndex,
                int timestampType,
                boolean hasPrevFill
        ) {
            this.timestampSampler = timestampSampler;
            this.fromFunc = fromFunc;
            this.toFunc = toFunc;
            this.fillModes = fillModes;
            this.constantFills = constantFills;
            this.timestampIndex = timestampIndex;
            this.timestampDriver = ColumnType.getTimestampDriver(timestampType);
            this.columnCount = metadata.getColumnCount();
            this.fillTimestampFunc = new FillTimestampConstant(timestampType);
            this.hasPrevFill = hasPrevFill;
            this.columnTypes = new short[columnCount];
            for (int i = 0; i < columnCount; i++) {
                columnTypes[i] = ColumnType.tagOf(metadata.getColumnType(i));
            }
            this.simplePrev = hasPrevFill ? new long[columnCount] : null;
            // Keyed support will be added incrementally.
            // For now, treat all queries as non-keyed (no cartesian product).
            this.isNonKeyed = true;
        }

        @Override
        public void close() {
            baseCursor = Misc.free(baseCursor);
        }

        @Override
        public Record getRecord() {
            return fillRecord;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return baseCursor.getSymbolTable(columnIndex);
        }

        // Pending row state
        private boolean hasPendingRow;
        private long pendingTs;
        private boolean baseCursorExhausted;
        private boolean hasExplicitTo;

        @Override
        public boolean hasNext() {
            if (!isInitialized) {
                initialize();
                isInitialized = true;
            }

            while (nextBucketTimestamp < maxTimestamp) {
                // Try to get the next data row's timestamp
                long dataTs;
                if (hasPendingRow) {
                    dataTs = pendingTs;
                } else if (!baseCursorExhausted && baseCursor.hasNext()) {
                    dataTs = baseRecord.getTimestamp(timestampIndex);
                    hasPendingRow = true;
                    pendingTs = dataTs;
                } else {
                    baseCursorExhausted = true;
                    dataTs = Long.MAX_VALUE;
                }

                // Early exit: base cursor exhausted and no explicit TO bound —
                // stop emitting because there are no more data rows and no
                // trailing fill range was requested. This check must come
                // AFTER the data fetch above so baseCursorExhausted is set
                // when the base cursor returns no more rows.
                if (baseCursorExhausted && !hasExplicitTo) {
                    return false;
                }

                if (dataTs == nextBucketTimestamp) {
                    // Data row at expected bucket — emit it.
                    // Do NOT peek ahead here: calling baseCursor.hasNext()
                    // would advance the base record, corrupting the current
                    // row's data before the caller reads it. The next call
                    // to hasNext() will fetch the next row at the top of
                    // the loop.
                    hasPendingRow = false;
                    fillRecord.isGapFilling = false;
                    if (hasPrevFill) {
                        savePrevValues(baseRecord);
                    }
                    nextBucketTimestamp = timestampSampler.nextTimestamp(nextBucketTimestamp);
                    return true;
                }

                if (dataTs > nextBucketTimestamp) {
                    // Gap — emit fill row, keep pending row for next iteration
                    fillRecord.isGapFilling = true;
                    fillTimestampFunc.value = nextBucketTimestamp;
                    nextBucketTimestamp = timestampSampler.nextTimestamp(nextBucketTimestamp);
                    return true;
                }

                if (dataTs < nextBucketTimestamp && hasPendingRow) {
                    // Data timestamp is before expected bucket (can happen with DST fall-back
                    // where timestamp_floor_utc produces non-monotonic UTC timestamps).
                    // Emit the data row as-is and advance.
                    hasPendingRow = false;
                    fillRecord.isGapFilling = false;
                    if (hasPrevFill) {
                        savePrevValues(baseRecord);
                    }
                    return true;
                }

                // Unreachable: all three cases (==, >, <) are handled above.
                // If we get here, it means dataTs is Long.MAX_VALUE and
                // hasExplicitTo is true — the gap branch above handles it.
            }
            return false;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            if (baseCursor != null) {
                baseCursor.toTop();
            }
            isInitialized = false;
            hasSimplePrev = false;
            hasPendingRow = false;
            baseCursorExhausted = false;
            hasExplicitTo = false;
        }

        private void initialize() {
            TimestampDriver driver = timestampDriver;
            long fromTs = fromFunc == driver.getTimestampConstantNull() ? Numbers.LONG_NULL
                    : driver.from(fromFunc.getTimestamp(null), ColumnType.getTimestampType(fromFunc.getType()));
            hasExplicitTo = toFunc != driver.getTimestampConstantNull();
            maxTimestamp = hasExplicitTo
                    ? driver.from(toFunc.getTimestamp(null), ColumnType.getTimestampType(toFunc.getType()))
                    : Numbers.LONG_NULL;

            // Peek first row to determine range
            if (baseCursor.hasNext()) {
                long firstTs = baseRecord.getTimestamp(timestampIndex);
                if (fromTs == Numbers.LONG_NULL || firstTs < fromTs) {
                    nextBucketTimestamp = firstTs;
                } else {
                    nextBucketTimestamp = fromTs;
                }
                timestampSampler.setStart(nextBucketTimestamp);
                hasPendingRow = true;
                pendingTs = firstTs;
                if (maxTimestamp == Numbers.LONG_NULL) {
                    // No TO — we'll stop after last data row + no trailing fill
                    maxTimestamp = Long.MAX_VALUE;
                }
            } else {
                if (fromTs != Numbers.LONG_NULL && maxTimestamp != Numbers.LONG_NULL) {
                    nextBucketTimestamp = fromTs;
                    timestampSampler.setStart(nextBucketTimestamp);
                } else {
                    maxTimestamp = Long.MIN_VALUE;
                    nextBucketTimestamp = Long.MAX_VALUE;
                }
                baseCursorExhausted = true;
            }
        }

        private void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
            this.baseCursor = baseCursor;
            this.baseRecord = baseCursor.getRecord();
            Function.init(constantFills, baseCursor, executionContext, null);
            fromFunc.init(baseCursor, executionContext);
            toFunc.init(baseCursor, executionContext);
            toTop();
        }

        private void savePrevValues(Record record) {
            hasSimplePrev = true;
            for (int i = 0; i < columnCount; i++) {
                if (i == timestampIndex) continue;
                simplePrev[i] = readColumnAsLongBits(record, i, columnTypes[i]);
            }
        }

        private static long readColumnAsLongBits(Record record, int col, short type) {
            return switch (type) {
                case ColumnType.DOUBLE -> Double.doubleToRawLongBits(record.getDouble(col));
                case ColumnType.FLOAT -> Float.floatToRawIntBits(record.getFloat(col));
                case ColumnType.INT, ColumnType.IPv4, ColumnType.GEOINT -> record.getInt(col);
                case ColumnType.SHORT, ColumnType.GEOSHORT -> record.getShort(col);
                case ColumnType.BYTE, ColumnType.GEOBYTE -> record.getByte(col);
                case ColumnType.BOOLEAN -> record.getBool(col) ? 1 : 0;
                case ColumnType.CHAR -> record.getChar(col);
                default -> record.getLong(col);
            };
        }

        private int fillMode(int col) {
            return fillModes.getQuick(col);
        }

        private long prevValue(int col) {
            int mode = fillMode(col);
            if (mode == FILL_PREV_SELF) return simplePrev[col];
            if (mode >= 0) return simplePrev[mode];
            return Numbers.LONG_NULL;
        }

        private class FillRecord implements Record {
            boolean isGapFilling;

            @Override
            public double getDouble(int col) {
                if (!isGapFilling) return baseRecord.getDouble(col);
                int mode = fillMode(col);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasSimplePrev) return Double.longBitsToDouble(prevValue(col));
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getDouble(null);
                return Double.NaN;
            }

            @Override
            public float getFloat(int col) {
                if (!isGapFilling) return baseRecord.getFloat(col);
                int mode = fillMode(col);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasSimplePrev) return Float.intBitsToFloat((int) prevValue(col));
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getFloat(null);
                return Float.NaN;
            }

            @Override
            public int getInt(int col) {
                if (!isGapFilling) return baseRecord.getInt(col);
                int mode = fillMode(col);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasSimplePrev) return (int) prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getInt(null);
                return Numbers.INT_NULL;
            }

            @Override
            public long getLong(int col) {
                if (!isGapFilling) return baseRecord.getLong(col);
                int mode = fillMode(col);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasSimplePrev) return prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getLong(null);
                return Numbers.LONG_NULL;
            }

            @Override
            public short getShort(int col) {
                if (!isGapFilling) return baseRecord.getShort(col);
                int mode = fillMode(col);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasSimplePrev) return (short) prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getShort(null);
                return 0;
            }

            @Override
            public byte getByte(int col) {
                if (!isGapFilling) return baseRecord.getByte(col);
                int mode = fillMode(col);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasSimplePrev) return (byte) prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getByte(null);
                return 0;
            }

            @Override
            public boolean getBool(int col) {
                if (!isGapFilling) return baseRecord.getBool(col);
                int mode = fillMode(col);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasSimplePrev) return prevValue(col) != 0;
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getBool(null);
                return false;
            }

            @Override
            public char getChar(int col) {
                if (!isGapFilling) return baseRecord.getChar(col);
                int mode = fillMode(col);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasSimplePrev) return (char) prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getChar(null);
                return 0;
            }

            @Override
            public long getTimestamp(int col) {
                if (!isGapFilling) return baseRecord.getTimestamp(col);
                if (col == timestampIndex) return fillTimestampFunc.value;
                int mode = fillMode(col);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasSimplePrev) return prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getLong(null);
                return Numbers.LONG_NULL;
            }

            @Override
            public io.questdb.cairo.arr.ArrayView getArray(int col, int columnType) {
                if (!isGapFilling) return baseRecord.getArray(col, columnType);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getArray(null);
                return null;
            }

            @Override
            public io.questdb.std.BinarySequence getBin(int col) {
                if (!isGapFilling) return baseRecord.getBin(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getBin(null);
                return null;
            }

            @Override
            public long getBinLen(int col) {
                if (!isGapFilling) return baseRecord.getBinLen(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getBinLen(null);
                return -1;
            }

            @Override
            public void getDecimal128(int col, io.questdb.std.Decimal128 sink) {
                if (!isGapFilling) { baseRecord.getDecimal128(col, sink); return; }
                if (fillMode(col) == FILL_CONSTANT) { constantFills.getQuick(col).getDecimal128(null, sink); return; }
            }

            @Override
            public short getDecimal16(int col) {
                if (!isGapFilling) return baseRecord.getDecimal16(col);
                int mode = fillMode(col);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasSimplePrev) return (short) prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getDecimal16(null);
                return 0;
            }

            @Override
            public void getDecimal256(int col, io.questdb.std.Decimal256 sink) {
                if (!isGapFilling) { baseRecord.getDecimal256(col, sink); return; }
                if (fillMode(col) == FILL_CONSTANT) { constantFills.getQuick(col).getDecimal256(null, sink); return; }
            }

            @Override
            public int getDecimal32(int col) {
                if (!isGapFilling) return baseRecord.getDecimal32(col);
                int mode = fillMode(col);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasSimplePrev) return (int) prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getDecimal32(null);
                return Numbers.INT_NULL;
            }

            @Override
            public long getDecimal64(int col) {
                if (!isGapFilling) return baseRecord.getDecimal64(col);
                int mode = fillMode(col);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasSimplePrev) return prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getDecimal64(null);
                return Numbers.LONG_NULL;
            }

            @Override
            public byte getDecimal8(int col) {
                if (!isGapFilling) return baseRecord.getDecimal8(col);
                int mode = fillMode(col);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasSimplePrev) return (byte) prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getDecimal8(null);
                return 0;
            }

            @Override
            public byte getGeoByte(int col) {
                if (!isGapFilling) return baseRecord.getGeoByte(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getGeoByte(null);
                return 0;
            }

            @Override
            public int getGeoInt(int col) {
                if (!isGapFilling) return baseRecord.getGeoInt(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getGeoInt(null);
                return Numbers.INT_NULL;
            }

            @Override
            public long getGeoLong(int col) {
                if (!isGapFilling) return baseRecord.getGeoLong(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getGeoLong(null);
                return Numbers.LONG_NULL;
            }

            @Override
            public short getGeoShort(int col) {
                if (!isGapFilling) return baseRecord.getGeoShort(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getGeoShort(null);
                return 0;
            }

            @Override
            public int getIPv4(int col) {
                if (!isGapFilling) return baseRecord.getIPv4(col);
                int mode = fillMode(col);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasSimplePrev) return (int) prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getIPv4(null);
                return Numbers.IPv4_NULL;
            }

            @Override
            public long getLong128Hi(int col) {
                if (!isGapFilling) return baseRecord.getLong128Hi(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getLong128Hi(null);
                return Numbers.LONG_NULL;
            }

            @Override
            public long getLong128Lo(int col) {
                if (!isGapFilling) return baseRecord.getLong128Lo(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getLong128Lo(null);
                return Numbers.LONG_NULL;
            }

            @Override
            public void getLong256(int col, io.questdb.std.str.CharSink<?> sink) {
                if (!isGapFilling) { baseRecord.getLong256(col, sink); return; }
                if (fillMode(col) == FILL_CONSTANT) { constantFills.getQuick(col).getLong256(null, sink); return; }
            }

            @Override
            public io.questdb.std.Long256 getLong256A(int col) {
                if (!isGapFilling) return baseRecord.getLong256A(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getLong256A(null);
                return Long256Impl.NULL_LONG256;
            }

            @Override
            public io.questdb.std.Long256 getLong256B(int col) {
                if (!isGapFilling) return baseRecord.getLong256B(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getLong256B(null);
                return Long256Impl.NULL_LONG256;
            }

            @Override
            public CharSequence getStrA(int col) {
                if (!isGapFilling) return baseRecord.getStrA(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getStrA(null);
                return null;
            }

            @Override
            public CharSequence getStrB(int col) {
                if (!isGapFilling) return baseRecord.getStrB(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getStrB(null);
                return null;
            }

            @Override
            public int getStrLen(int col) {
                if (!isGapFilling) return baseRecord.getStrLen(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getStrLen(null);
                return -1;
            }

            @Override
            public CharSequence getSymA(int col) {
                if (!isGapFilling) return baseRecord.getSymA(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getSymbol(null);
                return null;
            }

            @Override
            public CharSequence getSymB(int col) {
                if (!isGapFilling) return baseRecord.getSymB(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getSymbolB(null);
                return null;
            }

            @Override
            public io.questdb.std.str.Utf8Sequence getVarcharA(int col) {
                if (!isGapFilling) return baseRecord.getVarcharA(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getVarcharA(null);
                return null;
            }

            @Override
            public io.questdb.std.str.Utf8Sequence getVarcharB(int col) {
                if (!isGapFilling) return baseRecord.getVarcharB(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getVarcharB(null);
                return null;
            }

            @Override
            public int getVarcharSize(int col) {
                if (!isGapFilling) return baseRecord.getVarcharSize(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getVarcharSize(null);
                return -1;
            }
        }

        private static class FillTimestampConstant extends TimestampFunction implements ConstantFunction {
            long value;

            FillTimestampConstant(int timestampType) {
                super(timestampType);
            }

            @Override
            public long getTimestamp(Record rec) {
                return value;
            }
        }
    }
}
