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

package io.questdb.cairo;

import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.std.Chars;
import io.questdb.std.Long256Impl;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8SplitString;

/**
 * {@link SortedRowSink} that appends the globally sorted row stream into a
 * caller-supplied {@link TableWriter}. The sink is non-owning: the caller
 * constructs the writer, hands it to this sink, and is responsible for
 * {@link TableWriter#commit()}, {@link TableWriter#rollback()} and
 * {@link TableWriter#close()}. The sink never issues DDL, never commits and
 * never closes the writer.
 * <p>
 * Column dispatch uses the parquet column types surfaced by
 * {@link PartitionDecoder.Metadata}. Every non-timestamp parquet column must
 * have a name + type match in the writer's {@link TableMetadata}; the
 * designated timestamp of the writer must match the parquet timestamp column
 * by name. Mismatches throw {@link CairoException} from {@link #onStart}.
 * <p>
 * Lifecycle expectations follow {@link SortedRowSink}: exactly one
 * {@link #onStart} call, zero or more {@link #acceptRow} calls in ascending
 * timestamp order, exactly one {@link #onFinish} call. Phase B drives the
 * sink from a single thread so no internal synchronisation is required.
 * <p>
 * Unsupported column types (ARRAY, DECIMAL*, INTERVAL) are rejected in
 * {@link #onStart} with a descriptive error rather than during
 * {@link #acceptRow}.
 */
public class TableWriterSink implements SortedRowSink {

    private final Long256Impl long256Scratch = new Long256Impl();
    private final DirectString strView = new DirectString();
    private final Utf8SplitString varcharView = new Utf8SplitString();
    private final TableWriter writer;
    private int columnCount;
    private int[] parquetColTypes;
    private int[] parquetToWriterCol;
    private int tsColumnIndex = -1;

    public TableWriterSink(TableWriter writer) {
        if (writer == null) {
            throw CairoException.nonCritical().put("TableWriterSink requires a non-null TableWriter");
        }
        this.writer = writer;
    }

    @Override
    public void acceptRow(RowGroupBuffers src, int row, long ts) {
        final TableWriter.Row r = writer.newRow(ts);
        for (int c = 0; c < columnCount; c++) {
            if (c == tsColumnIndex) {
                continue;
            }
            final int writerCol = parquetToWriterCol[c];
            final int type = parquetColTypes[c];
            final long dataPtr = src.getChunkDataPtr(c);
            final long dataSize = src.getChunkDataSize(c);
            final long auxPtr = src.getChunkAuxPtr(c);
            final long auxSize = src.getChunkAuxSize(c);
            dispatchPut(r, writerCol, type, dataPtr, dataSize, auxPtr, auxSize, row);
        }
        r.append();
    }

    @Override
    public void close() {
        parquetColTypes = null;
        parquetToWriterCol = null;
        columnCount = 0;
        tsColumnIndex = -1;
    }

    @Override
    public void onFinish() {
        // Non-owning sink: commit is the caller's responsibility.
    }

    @Override
    public void onStart(PartitionDecoder.Metadata meta, int tsColumnIndex, long totalRows) {
        this.tsColumnIndex = tsColumnIndex;
        this.columnCount = meta.getColumnCount();
        this.parquetColTypes = new int[columnCount];
        this.parquetToWriterCol = new int[columnCount];

        final TableMetadata writerMeta = writer.getMetadata();
        final int writerTsIdx = writerMeta.getTimestampIndex();
        if (writerTsIdx < 0) {
            throw CairoException.nonCritical()
                    .put("destination table has no designated timestamp column");
        }

        final CharSequence parquetTsName = meta.getColumnName(tsColumnIndex);
        final String writerTsName = writerMeta.getColumnName(writerTsIdx);
        if (!Chars.equalsNullable(parquetTsName, writerTsName)) {
            throw CairoException.nonCritical()
                    .put("timestamp column name mismatch [parquet=")
                    .put(parquetTsName)
                    .put(", writer=")
                    .put(writerTsName)
                    .put(']');
        }

        for (int c = 0; c < columnCount; c++) {
            final int parquetType = meta.getColumnType(c);
            parquetColTypes[c] = parquetType;
            final CharSequence name = meta.getColumnName(c);
            if (c == tsColumnIndex) {
                parquetToWriterCol[c] = writerTsIdx;
                continue;
            }
            final int writerCol = writerMeta.getColumnIndexQuiet(name);
            if (writerCol < 0) {
                throw CairoException.nonCritical()
                        .put("parquet column not found in destination table [column=")
                        .put(name)
                        .put(']');
            }
            final int writerType = writerMeta.getColumnType(writerCol);
            if (writerType != parquetType) {
                throw CairoException.nonCritical()
                        .put("column type mismatch [column=").put(name)
                        .put(", parquet=").put(ColumnType.nameOf(parquetType))
                        .put(", writer=").put(ColumnType.nameOf(writerType))
                        .put(']');
            }
            ensureSupported(name, parquetType);
            parquetToWriterCol[c] = writerCol;
        }
    }

    private void dispatchPut(
            TableWriter.Row r,
            int writerCol,
            int type,
            long dataPtr,
            long dataSize,
            long auxPtr,
            long auxSize,
            int row
    ) {
        switch (ColumnType.tagOf(type)) {
            case ColumnType.BOOLEAN ->
                    r.putBool(writerCol, Unsafe.getUnsafe().getByte(dataPtr + row) != 0);
            case ColumnType.BYTE ->
                    r.putByte(writerCol, Unsafe.getUnsafe().getByte(dataPtr + row));
            case ColumnType.SHORT ->
                    r.putShort(writerCol, Unsafe.getUnsafe().getShort(dataPtr + (long) row * 2L));
            case ColumnType.CHAR ->
                    r.putChar(writerCol, Unsafe.getUnsafe().getChar(dataPtr + (long) row * 2L));
            case ColumnType.INT ->
                    r.putInt(writerCol, Unsafe.getUnsafe().getInt(dataPtr + (long) row * 4L));
            case ColumnType.LONG ->
                    r.putLong(writerCol, Unsafe.getUnsafe().getLong(dataPtr + (long) row * 8L));
            case ColumnType.DATE ->
                    r.putDate(writerCol, Unsafe.getUnsafe().getLong(dataPtr + (long) row * 8L));
            case ColumnType.TIMESTAMP ->
                    r.putTimestamp(writerCol, Unsafe.getUnsafe().getLong(dataPtr + (long) row * 8L));
            case ColumnType.FLOAT ->
                    r.putFloat(writerCol, Unsafe.getUnsafe().getFloat(dataPtr + (long) row * 4L));
            case ColumnType.DOUBLE ->
                    r.putDouble(writerCol, Unsafe.getUnsafe().getDouble(dataPtr + (long) row * 8L));
            case ColumnType.IPv4 ->
                    r.putIPv4(writerCol, Unsafe.getUnsafe().getInt(dataPtr + (long) row * 4L));
            case ColumnType.SYMBOL ->
                    // Parquet runs preserve the original symbol dictionary key as INT.
                    r.putSymIndex(writerCol, Unsafe.getUnsafe().getInt(dataPtr + (long) row * 4L));
            case ColumnType.GEOBYTE ->
                    r.putGeoHash(writerCol, Unsafe.getUnsafe().getByte(dataPtr + row));
            case ColumnType.GEOSHORT ->
                    r.putGeoHash(writerCol, Unsafe.getUnsafe().getShort(dataPtr + (long) row * 2L));
            case ColumnType.GEOINT ->
                    r.putGeoHash(writerCol, Unsafe.getUnsafe().getInt(dataPtr + (long) row * 4L));
            case ColumnType.GEOLONG ->
                    r.putGeoHash(writerCol, Unsafe.getUnsafe().getLong(dataPtr + (long) row * 8L));
            case ColumnType.UUID -> {
                final long base = dataPtr + (long) row * 16L;
                final long lo = Unsafe.getUnsafe().getLong(base);
                final long hi = Unsafe.getUnsafe().getLong(base + 8);
                r.putLong128(writerCol, lo, hi);
            }
            case ColumnType.LONG256 -> {
                final long base = dataPtr + (long) row * 32L;
                long256Scratch.setAll(
                        Unsafe.getUnsafe().getLong(base),
                        Unsafe.getUnsafe().getLong(base + 8),
                        Unsafe.getUnsafe().getLong(base + 16),
                        Unsafe.getUnsafe().getLong(base + 24)
                );
                r.putLong256(writerCol, long256Scratch);
            }
            case ColumnType.STRING -> {
                final long offset = Unsafe.getUnsafe().getLong(auxPtr + (long) row * 8L);
                final long valAddr = dataPtr + offset;
                final int charLen = Unsafe.getUnsafe().getInt(valAddr);
                if (charLen == TableUtils.NULL_LEN) {
                    r.putStr(writerCol, (CharSequence) null);
                } else {
                    strView.of(valAddr + 4, charLen);
                    r.putStr(writerCol, strView);
                }
            }
            case ColumnType.VARCHAR -> {
                final long auxLim = auxPtr + auxSize;
                final long dataLim = dataPtr + dataSize;
                final Utf8Sequence v = VarcharTypeDriver.getSplitValue(
                        auxPtr, auxLim, dataPtr, dataLim, row, varcharView
                );
                r.putVarchar(writerCol, v);
            }
            case ColumnType.BINARY -> {
                final long offset = Unsafe.getUnsafe().getLong(auxPtr + (long) row * 8L);
                final long valAddr = dataPtr + offset;
                final long len = Unsafe.getUnsafe().getLong(valAddr);
                if (len != TableUtils.NULL_LEN) {
                    r.putBin(writerCol, valAddr + 8, len);
                }
                // NULL BINARY: leave the default null-setter in place by not
                // calling putBin at all; both putBin overloads unconditionally
                // mark the row as non-null.
            }
            default -> throw CairoException.nonCritical()
                    .put("unsupported column type in TableWriterSink [type=")
                    .put(ColumnType.nameOf(type))
                    .put(']');
        }
    }

    private void ensureSupported(CharSequence name, int type) {
        switch (ColumnType.tagOf(type)) {
            case ColumnType.BOOLEAN,
                 ColumnType.BYTE,
                 ColumnType.SHORT,
                 ColumnType.CHAR,
                 ColumnType.INT,
                 ColumnType.LONG,
                 ColumnType.DATE,
                 ColumnType.TIMESTAMP,
                 ColumnType.FLOAT,
                 ColumnType.DOUBLE,
                 ColumnType.IPv4,
                 ColumnType.SYMBOL,
                 ColumnType.GEOBYTE,
                 ColumnType.GEOSHORT,
                 ColumnType.GEOINT,
                 ColumnType.GEOLONG,
                 ColumnType.UUID,
                 ColumnType.LONG256,
                 ColumnType.STRING,
                 ColumnType.VARCHAR,
                 ColumnType.BINARY -> {
            }
            default -> throw CairoException.nonCritical()
                    .put("unsupported column type in TableWriterSink [column=")
                    .put(name)
                    .put(", type=").put(ColumnType.nameOf(type))
                    .put(']');
        }
    }
}
