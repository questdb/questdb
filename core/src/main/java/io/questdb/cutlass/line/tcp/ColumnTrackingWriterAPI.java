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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.AlterTableContextException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.std.BinarySequence;
import io.questdb.std.BoolList;
import io.questdb.std.Long256;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectByteCharSequence;
import org.jetbrains.annotations.NotNull;

/**
 * Wraps given {@link TableWriterAPI} and tracks columns written via {@link TableWriter.Row}
 * since the last commit/rollback for the purpose of further permission tracking.
 */
public class ColumnTrackingWriterAPI implements TableWriterAPI {
    private final TableWriterAPI delegate;
    private final Row row = new Row();
    private final ObjList<CharSequence> writtenColumnNames = new ObjList<>();

    public ColumnTrackingWriterAPI(TableWriterAPI delegate) {
        this.delegate = delegate;
    }

    @Override
    public void addColumn(@NotNull CharSequence columnName, int columnType) {
        delegate.addColumn(columnName, columnType);
    }

    @Override
    public void addColumn(
            CharSequence columnName,
            int columnType,
            int symbolCapacity,
            boolean symbolCacheFlag,
            boolean isIndexed,
            int indexValueBlockCapacity
    ) {
        delegate.addColumn(columnName, columnType, symbolCapacity, symbolCacheFlag, isIndexed, indexValueBlockCapacity);
    }

    @Override
    public long apply(AlterOperation alterOp, boolean contextAllowsAnyStructureChanges) throws AlterTableContextException {
        return delegate.apply(alterOp, contextAllowsAnyStructureChanges);
    }

    @Override
    public long apply(UpdateOperation operation) {
        return delegate.apply(operation);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public long commit() {
        row.clearWrittenCols();
        return delegate.commit();
    }

    @Override
    public TableRecordMetadata getMetadata() {
        return delegate.getMetadata();
    }

    @Override
    public long getMetadataVersion() {
        return delegate.getMetadataVersion();
    }

    @Override
    public int getSymbolCountWatermark(int columnIndex) {
        return delegate.getSymbolCountWatermark(columnIndex);
    }

    @Override
    public TableToken getTableToken() {
        return delegate.getTableToken();
    }

    @Override
    public long getUncommittedRowCount() {
        return delegate.getUncommittedRowCount();
    }

    public ObjList<CharSequence> getWrittenColumnNames() {
        writtenColumnNames.clear();
        final BoolList writtenCols = row.writtenCols;
        final TableRecordMetadata metadata = delegate.getMetadata();
        for (int i = 0, n = Math.min(metadata.getColumnCount(), writtenCols.size()); i < n; i++) {
            if (writtenCols.getQuiet(i)) {
                writtenColumnNames.add(metadata.getColumnName(i));
            }
        }
        return writtenColumnNames;
    }

    @Override
    public void ic() {
        row.clearWrittenCols();
        delegate.ic();
    }

    @Override
    public void ic(long o3MaxLag) {
        row.clearWrittenCols();
        delegate.ic(o3MaxLag);
    }

    @Override
    public TableWriter.Row newRow() {
        return row.of(delegate.newRow());
    }

    @Override
    public TableWriter.Row newRow(long timestamp) {
        return row.of(delegate.newRow(timestamp));
    }

    @Override
    public void rollback() {
        row.clearWrittenCols();
        delegate.rollback();
    }

    @Override
    public boolean supportsMultipleWriters() {
        return delegate.supportsMultipleWriters();
    }

    @Override
    public void truncate() {
        row.clearWrittenCols();
        delegate.truncate();
    }

    @Override
    public void truncateSoft() {
        row.clearWrittenCols();
        delegate.truncateSoft();
    }

    private static class Row implements TableWriter.Row {
        final BoolList writtenCols = new BoolList();
        private TableWriter.Row delegate;

        @Override
        public void append() {
            delegate.append();
        }

        @Override
        public void cancel() {
            delegate.cancel();
        }

        public void clearWrittenCols() {
            writtenCols.setAll(writtenCols.size(), false);
        }

        public Row of(TableWriter.Row delegate) {
            this.delegate = delegate;
            return this;
        }

        @Override
        public void putBin(int columnIndex, long address, long len) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putBin(columnIndex, address, len);
        }

        @Override
        public void putBin(int columnIndex, BinarySequence sequence) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putBin(columnIndex, sequence);
        }

        @Override
        public void putBool(int columnIndex, boolean value) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putBool(columnIndex, value);
        }

        @Override
        public void putByte(int columnIndex, byte value) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putByte(columnIndex, value);
        }

        @Override
        public void putChar(int columnIndex, char value) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putChar(columnIndex, value);
        }

        @Override
        public void putDate(int columnIndex, long value) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putDate(columnIndex, value);
        }

        @Override
        public void putDouble(int columnIndex, double value) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putDouble(columnIndex, value);
        }

        @Override
        public void putFloat(int columnIndex, float value) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putFloat(columnIndex, value);
        }

        @Override
        public void putGeoHash(int columnIndex, long value) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putGeoHash(columnIndex, value);
        }

        @Override
        public void putGeoHashDeg(int columnIndex, double lat, double lon) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putGeoHashDeg(columnIndex, lat, lon);
        }

        @Override
        public void putGeoStr(int columnIndex, CharSequence value) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putGeoStr(columnIndex, value);
        }

        @Override
        public void putInt(int columnIndex, int value) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putInt(columnIndex, value);
        }

        @Override
        public void putLong(int columnIndex, long value) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putLong(columnIndex, value);
        }

        @Override
        public void putLong128(int columnIndex, long lo, long hi) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putLong128(columnIndex, lo, hi);
        }

        @Override
        public void putLong256(int columnIndex, long l0, long l1, long l2, long l3) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putLong256(columnIndex, l0, l1, l2, l3);
        }

        @Override
        public void putLong256(int columnIndex, Long256 value) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putLong256(columnIndex, value);
        }

        @Override
        public void putLong256(int columnIndex, CharSequence hexString) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putLong256(columnIndex, hexString);
        }

        @Override
        public void putLong256(int columnIndex, @NotNull CharSequence hexString, int start, int end) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putLong256(columnIndex, hexString, start, end);
        }

        @Override
        public void putShort(int columnIndex, short value) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putShort(columnIndex, value);
        }

        @Override
        public void putStr(int columnIndex, CharSequence value) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putStr(columnIndex, value);
        }

        @Override
        public void putStr(int columnIndex, char value) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putStr(columnIndex, value);
        }

        @Override
        public void putStr(int columnIndex, CharSequence value, int pos, int len) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putStr(columnIndex, value, pos, len);
        }

        @Override
        public void putStrUtf8AsUtf16(int columnIndex, DirectByteCharSequence value, boolean hasNonAsciiChars) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putStrUtf8AsUtf16(columnIndex, value, hasNonAsciiChars);
        }

        @Override
        public void putSym(int columnIndex, CharSequence value) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putSym(columnIndex, value);
        }

        @Override
        public void putSym(int columnIndex, char value) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putSym(columnIndex, value);
        }

        @Override
        public void putSymIndex(int columnIndex, int key) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putInt(columnIndex, key);
        }

        @Override
        public void putSymUtf8(int columnIndex, DirectByteCharSequence value, boolean hasNonAsciiChars) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putSymUtf8(columnIndex, value, hasNonAsciiChars);
        }

        @Override
        public void putTimestamp(int columnIndex, long value) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putTimestamp(columnIndex, value);
        }

        @Override
        public void putUuid(int columnIndex, CharSequence uuid) {
            writtenCols.extendAndReplace(columnIndex, true);
            delegate.putUuid(columnIndex, uuid);
        }
    }
}
