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
import io.questdb.cairo.RecordChain;
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
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.Long256Impl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;

/**
 * Unified fill cursor for SAMPLE BY on the GROUP BY fast path. Handles both
 * keyed and non-keyed queries with FILL(NULL), FILL(VALUE), and FILL(PREV).
 * <p>
 * Buffers GROUP BY output into a {@link RecordChain}, sorts by timestamp,
 * and emits rows in timestamp order with gap filling. For non-keyed queries
 * each bucket has at most one row. For keyed queries, multiple rows per bucket.
 * <p>
 * Note: this version does NOT implement cartesian-product keyed fill (emitting
 * all keys for every bucket). It fills whole-bucket gaps only. Per-key fill
 * requires additional Map infrastructure and will be added incrementally.
 * <p>
 * Reports {@link #followedOrderByAdvice()} = true because output is emitted
 * in timestamp order.
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
                timestampIndex, timestampType, hasPrevFill,
                recordSink
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
        Misc.free(cursor);
        Misc.free(fromFunc);
        Misc.free(toFunc);
        Misc.freeObjList(constantFills);
    }

    private static class SampleByFillCursor implements NoRandomAccessRecordCursor {
        private final CairoConfiguration configuration;
        private final int columnCount;
        private final short[] columnTypes;
        private final ObjList<Function> constantFills;
        private final FillRecord fillRecord = new FillRecord();
        private final FillTimestampConstant fillTimestampFunc;
        private final IntList fillModes;
        private final Function fromFunc;
        private final boolean hasPrevFill;
        private final long[] prevValues;
        private final RecordMetadata metadata;
        private final RecordSink recordSink;
        private final TimestampDriver timestampDriver;
        private final int timestampIndex;
        private final TimestampSampler timestampSampler;
        private final Function toFunc;
        private RecordChain chain;
        private DirectLongList sortedIndex; // pairs: (timestamp, chainRowId)
        private boolean isBuffered;
        private boolean hasPrev;
        private long maxTimestamp;
        private long nextBucketTimestamp;
        private long sortedPos;
        private long sortedSize;
        private RecordCursor baseCursor;

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
                boolean hasPrevFill,
                RecordSink recordSink
        ) {
            this.configuration = configuration;
            this.metadata = metadata;
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
            this.prevValues = hasPrevFill ? new long[columnCount] : null;
            this.recordSink = recordSink;
            this.columnTypes = new short[columnCount];
            for (int i = 0; i < columnCount; i++) {
                columnTypes[i] = ColumnType.tagOf(metadata.getColumnType(i));
            }
        }

        @Override
        public void close() {
            baseCursor = Misc.free(baseCursor);
            chain = Misc.free(chain);
            sortedIndex = Misc.free(sortedIndex);
        }

        @Override
        public Record getRecord() {
            return fillRecord;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return baseCursor.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            if (!isBuffered) {
                bufferAndSort();
                isBuffered = true;
            }

            while (nextBucketTimestamp < maxTimestamp) {
                // Emit data rows at current bucket
                if (sortedPos < sortedSize) {
                    long dataTs = sortedIndex.get(sortedPos * 2);
                    if (dataTs == nextBucketTimestamp) {
                        long rowId = sortedIndex.get(sortedPos * 2 + 1);
                        chain.recordAt(fillRecord.dataRecord, rowId);
                        if (hasPrevFill) {
                            savePrevValues(fillRecord.dataRecord);
                        }
                        fillRecord.isGapFilling = false;
                        sortedPos++;
                        // Check if more rows at same timestamp (keyed)
                        if (sortedPos >= sortedSize || sortedIndex.get(sortedPos * 2) != nextBucketTimestamp) {
                            nextBucketTimestamp = timestampSampler.nextTimestamp(nextBucketTimestamp);
                        }
                        return true;
                    }
                }
                // Gap — emit fill row
                fillRecord.isGapFilling = true;
                fillTimestampFunc.value = nextBucketTimestamp;
                nextBucketTimestamp = timestampSampler.nextTimestamp(nextBucketTimestamp);
                return true;
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
            isBuffered = false;
            hasPrev = false;
            sortedPos = 0;
        }

        private void bufferAndSort() {
            if (chain == null) {
                chain = new RecordChain(
                        metadata, recordSink,
                        configuration.getSqlSortValuePageSize(),
                        configuration.getSqlSortValueMaxPages()
                );
            } else {
                chain.clear();
            }
            if (sortedIndex == null) {
                sortedIndex = new DirectLongList(16, MemoryTag.NATIVE_GROUP_BY_FUNCTION);
            } else {
                sortedIndex.clear();
            }

            chain.setSymbolTableResolver(baseCursor);
            fillRecord.dataRecord = chain.getRecord();

            final Record baseRecord = baseCursor.getRecord();
            while (baseCursor.hasNext()) {
                long rowId = chain.put(baseRecord, -1);
                long timestamp = baseRecord.getTimestamp(timestampIndex);
                sortedIndex.add(timestamp);
                sortedIndex.add(rowId);
            }

            sortedSize = sortedIndex.size() / 2;
            if (sortedSize > 1) {
                Vect.sortLongIndexAscInPlace(sortedIndex.getAddress(), sortedSize);
            }

            TimestampDriver driver = timestampDriver;
            long minTimestamp = fromFunc == driver.getTimestampConstantNull() ? Long.MAX_VALUE
                    : driver.from(fromFunc.getTimestamp(null), ColumnType.getTimestampType(fromFunc.getType()));
            maxTimestamp = toFunc == driver.getTimestampConstantNull() ? Long.MIN_VALUE
                    : driver.from(toFunc.getTimestamp(null), ColumnType.getTimestampType(toFunc.getType()));

            if (sortedSize > 0) {
                long firstTs = sortedIndex.get(0);
                long lastTs = sortedIndex.get((sortedSize - 1) * 2);
                minTimestamp = Math.min(minTimestamp, firstTs);
                maxTimestamp = Math.max(maxTimestamp, timestampSampler.nextTimestamp(lastTs));
            }

            if (minTimestamp == Long.MAX_VALUE) {
                maxTimestamp = Long.MIN_VALUE;
                nextBucketTimestamp = Long.MAX_VALUE;
                return;
            }

            timestampSampler.setStart(minTimestamp);
            nextBucketTimestamp = minTimestamp;
            sortedPos = 0;
        }

        private void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
            this.baseCursor = baseCursor;
            Function.init(constantFills, baseCursor, executionContext, null);
            fromFunc.init(baseCursor, executionContext);
            toFunc.init(baseCursor, executionContext);
            toTop();
        }

        private void savePrevValues(Record record) {
            hasPrev = true;
            for (int i = 0; i < columnCount; i++) {
                if (i == timestampIndex) continue;
                prevValues[i] = readColumnAsLongBits(record, i, columnTypes[i]);
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
            if (mode == FILL_PREV_SELF) return prevValues[col];
            if (mode >= 0) return prevValues[mode];
            return Numbers.LONG_NULL;
        }

        private class FillRecord implements Record {
            boolean isGapFilling;
            Record dataRecord;

            @Override
            public double getDouble(int col) {
                if (!isGapFilling) return dataRecord.getDouble(col);
                int mode = fillMode(col);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasPrev) return Double.longBitsToDouble(prevValue(col));
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getDouble(null);
                return Double.NaN;
            }

            @Override
            public float getFloat(int col) {
                if (!isGapFilling) return dataRecord.getFloat(col);
                int mode = fillMode(col);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasPrev) return Float.intBitsToFloat((int) prevValue(col));
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getFloat(null);
                return Float.NaN;
            }

            @Override
            public int getInt(int col) {
                if (!isGapFilling) return dataRecord.getInt(col);
                int mode = fillMode(col);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasPrev) return (int) prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getInt(null);
                return Numbers.INT_NULL;
            }

            @Override
            public long getLong(int col) {
                if (!isGapFilling) return dataRecord.getLong(col);
                int mode = fillMode(col);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasPrev) return prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getLong(null);
                return Numbers.LONG_NULL;
            }

            @Override
            public short getShort(int col) {
                if (!isGapFilling) return dataRecord.getShort(col);
                int mode = fillMode(col);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasPrev) return (short) prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getShort(null);
                return 0;
            }

            @Override
            public byte getByte(int col) {
                if (!isGapFilling) return dataRecord.getByte(col);
                int mode = fillMode(col);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasPrev) return (byte) prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getByte(null);
                return 0;
            }

            @Override
            public boolean getBool(int col) {
                if (!isGapFilling) return dataRecord.getBool(col);
                int mode = fillMode(col);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasPrev) return prevValue(col) != 0;
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getBool(null);
                return false;
            }

            @Override
            public char getChar(int col) {
                if (!isGapFilling) return dataRecord.getChar(col);
                int mode = fillMode(col);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasPrev) return (char) prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getChar(null);
                return 0;
            }

            @Override
            public long getTimestamp(int col) {
                if (!isGapFilling) return dataRecord.getTimestamp(col);
                if (col == timestampIndex) return fillTimestampFunc.value;
                int mode = fillMode(col);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasPrev) return prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getLong(null);
                return Numbers.LONG_NULL;
            }

            @Override
            public io.questdb.cairo.arr.ArrayView getArray(int col, int columnType) {
                if (!isGapFilling) return dataRecord.getArray(col, columnType);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getArray(null);
                return null;
            }

            @Override
            public io.questdb.std.BinarySequence getBin(int col) {
                if (!isGapFilling) return dataRecord.getBin(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getBin(null);
                return null;
            }

            @Override
            public long getBinLen(int col) {
                if (!isGapFilling) return dataRecord.getBinLen(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getBinLen(null);
                return -1;
            }

            @Override
            public void getDecimal128(int col, io.questdb.std.Decimal128 sink) {
                if (!isGapFilling) { dataRecord.getDecimal128(col, sink); return; }
                if (fillMode(col) == FILL_CONSTANT) { constantFills.getQuick(col).getDecimal128(null, sink); return; }
            }

            @Override
            public short getDecimal16(int col) {
                if (!isGapFilling) return dataRecord.getDecimal16(col);
                int mode = fillMode(col);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasPrev) return (short) prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getDecimal16(null);
                return 0;
            }

            @Override
            public void getDecimal256(int col, io.questdb.std.Decimal256 sink) {
                if (!isGapFilling) { dataRecord.getDecimal256(col, sink); return; }
                if (fillMode(col) == FILL_CONSTANT) { constantFills.getQuick(col).getDecimal256(null, sink); return; }
            }

            @Override
            public int getDecimal32(int col) {
                if (!isGapFilling) return dataRecord.getDecimal32(col);
                int mode = fillMode(col);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasPrev) return (int) prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getDecimal32(null);
                return Numbers.INT_NULL;
            }

            @Override
            public long getDecimal64(int col) {
                if (!isGapFilling) return dataRecord.getDecimal64(col);
                int mode = fillMode(col);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasPrev) return prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getDecimal64(null);
                return Numbers.LONG_NULL;
            }

            @Override
            public byte getDecimal8(int col) {
                if (!isGapFilling) return dataRecord.getDecimal8(col);
                int mode = fillMode(col);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasPrev) return (byte) prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getDecimal8(null);
                return 0;
            }

            @Override
            public byte getGeoByte(int col) {
                if (!isGapFilling) return dataRecord.getGeoByte(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getGeoByte(null);
                return 0;
            }

            @Override
            public int getGeoInt(int col) {
                if (!isGapFilling) return dataRecord.getGeoInt(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getGeoInt(null);
                return Numbers.INT_NULL;
            }

            @Override
            public long getGeoLong(int col) {
                if (!isGapFilling) return dataRecord.getGeoLong(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getGeoLong(null);
                return Numbers.LONG_NULL;
            }

            @Override
            public short getGeoShort(int col) {
                if (!isGapFilling) return dataRecord.getGeoShort(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getGeoShort(null);
                return 0;
            }

            @Override
            public int getIPv4(int col) {
                if (!isGapFilling) return dataRecord.getIPv4(col);
                int mode = fillMode(col);
                if ((mode == FILL_PREV_SELF || mode >= 0) && hasPrev) return (int) prevValue(col);
                if (mode == FILL_CONSTANT) return constantFills.getQuick(col).getIPv4(null);
                return Numbers.IPv4_NULL;
            }

            @Override
            public long getLong128Hi(int col) {
                if (!isGapFilling) return dataRecord.getLong128Hi(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getLong128Hi(null);
                return Numbers.LONG_NULL;
            }

            @Override
            public long getLong128Lo(int col) {
                if (!isGapFilling) return dataRecord.getLong128Lo(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getLong128Lo(null);
                return Numbers.LONG_NULL;
            }

            @Override
            public void getLong256(int col, io.questdb.std.str.CharSink<?> sink) {
                if (!isGapFilling) { dataRecord.getLong256(col, sink); return; }
                if (fillMode(col) == FILL_CONSTANT) { constantFills.getQuick(col).getLong256(null, sink); return; }
            }

            @Override
            public io.questdb.std.Long256 getLong256A(int col) {
                if (!isGapFilling) return dataRecord.getLong256A(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getLong256A(null);
                return Long256Impl.NULL_LONG256;
            }

            @Override
            public io.questdb.std.Long256 getLong256B(int col) {
                if (!isGapFilling) return dataRecord.getLong256B(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getLong256B(null);
                return Long256Impl.NULL_LONG256;
            }

            @Override
            public CharSequence getStrA(int col) {
                if (!isGapFilling) return dataRecord.getStrA(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getStrA(null);
                return null;
            }

            @Override
            public CharSequence getStrB(int col) {
                if (!isGapFilling) return dataRecord.getStrB(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getStrB(null);
                return null;
            }

            @Override
            public int getStrLen(int col) {
                if (!isGapFilling) return dataRecord.getStrLen(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getStrLen(null);
                return -1;
            }

            @Override
            public CharSequence getSymA(int col) {
                if (!isGapFilling) return dataRecord.getSymA(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getSymbol(null);
                return null;
            }

            @Override
            public CharSequence getSymB(int col) {
                if (!isGapFilling) return dataRecord.getSymB(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getSymbolB(null);
                return null;
            }

            @Override
            public io.questdb.std.str.Utf8Sequence getVarcharA(int col) {
                if (!isGapFilling) return dataRecord.getVarcharA(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getVarcharA(null);
                return null;
            }

            @Override
            public io.questdb.std.str.Utf8Sequence getVarcharB(int col) {
                if (!isGapFilling) return dataRecord.getVarcharB(col);
                if (fillMode(col) == FILL_CONSTANT) return constantFills.getQuick(col).getVarcharB(null);
                return null;
            }

            @Override
            public int getVarcharSize(int col) {
                if (!isGapFilling) return dataRecord.getVarcharSize(col);
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
