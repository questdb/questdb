/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.NullColumn;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.bind.CompiledFilterSymbolBindVariable;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.*;
import io.questdb.std.str.AbstractCharSequence;
import io.questdb.std.str.CharSink;

import java.util.function.BooleanSupplier;

class CompiledFilterRecordCursor implements RecordCursor {

    private final PageFrameRecord recordA;
    private final PageFrameRecord recordB;

    private PageFrameCursor pageFrameCursor;
    private RecordMetadata metadata;

    // Java based filter; used for page frames with present column tops
    private Function colTopsFilter;
    // JIT compiled filter; used for dense page frames (no column tops)
    private CompiledFilter compiledFilter;
    private DirectLongList rows;
    private DirectLongList columns;
    private MemoryCARW bindVarMemory;
    private int bindVarCount;

    private int pageFrameIndex;
    // The following fields are used for table iteration:
    // when compiled filter is in use, they store rows array indexes;
    // when Java filter is in use, they store rowids
    private long hi;
    private long current;

    private BooleanSupplier next;

    private final BooleanSupplier nextColTopsRow = this::nextColTopsRow;
    private final BooleanSupplier nextRow = this::nextRow;
    private final BooleanSupplier nextPage = this::nextPage;
    private final BooleanSupplier nextReenterPageFrame = this::nextReenterPageFrame;

    public CompiledFilterRecordCursor() {
        this.recordA = new PageFrameRecord();
        this.recordB = new PageFrameRecord();
    }

    public void of(
            RecordCursorFactory factory,
            Function filter,
            CompiledFilter compiledFilter,
            DirectLongList rows,
            DirectLongList columns,
            ObjList<Function> bindVarFunctions,
            MemoryCARW bindVarMemory,
            SqlExecutionContext executionContext
    ) throws SqlException {
        this.pageFrameIndex = -1;
        this.colTopsFilter = filter;
        this.compiledFilter = compiledFilter;
        this.rows = rows;
        this.columns = columns;
        this.pageFrameCursor = factory.getPageFrameCursor(executionContext);
        recordA.of(pageFrameCursor);
        recordB.of(pageFrameCursor);
        this.metadata = factory.getMetadata();
        this.next = nextPage;
        this.bindVarMemory = bindVarMemory;
        this.bindVarCount = bindVarFunctions.size();
        colTopsFilter.init(this, executionContext);
        prepareBindVarMemory(bindVarFunctions, executionContext);
    }

    private void prepareBindVarMemory(ObjList<Function> functions, SqlExecutionContext executionContext) throws SqlException {
        bindVarMemory.truncate();
        for (int i = 0, n = functions.size(); i < n; i++) {
            Function function = functions.getQuick(i);
            writeBindVarFunction(function, executionContext);
        }
    }

    @Override
    public void close() {
        pageFrameCursor.close();
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public Record getRecordB() {
        return recordB;
    }

    @Override
    public void recordAt(Record record, long rowId) {
        final int frameIndex = Rows.toPartitionIndex(rowId);
        final long index = Rows.toLocalRowID(rowId);

        seekPageFrame((PageFrameRecord) record, frameIndex);
        ((PageFrameRecord) record).setIndex(index);

        next = nextReenterPageFrame;
    }

    private PageFrame seekPageFrame(PageFrameRecord record, int toFrameIndex) {
        pageFrameCursor.toTop();

        int frameIndex = -1;
        PageFrame frame;
        while ((frame = pageFrameCursor.next()) != null) {
            frameIndex += 1;

            if (frameIndex == toFrameIndex) {
                record.jumpTo(frame, frameIndex);
                break;
            }
        }
        return frame;
    }

    private boolean nextReenterPageFrame() {
        if (pageFrameIndex == -1) {
            next = nextPage;
            return next.getAsBoolean();
        }

        PageFrame frame = seekPageFrame(recordA, pageFrameIndex);

        // Restore next
        if (hasColumnTops(frame)) {
            next = nextColTopsRow;
        } else {
            next = nextRow;
        }

        return next.getAsBoolean();
    }

    @Override
    public void toTop() {
        pageFrameIndex = -1;
        colTopsFilter.toTop();
        pageFrameCursor.toTop();
        next = nextPage;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return pageFrameCursor.getSymbolMapReader(columnIndex);
    }

    @Override
    public boolean hasNext() {
        return next.getAsBoolean();
    }

    @Override
    public long size() {
        return -1;
    }

    private boolean nextColTopsRow() {
        seekNextColTopsRow();
        if (current < hi) {
            return true;
        }
        return nextPage();
    }

    private void seekNextColTopsRow() {
        current += 1;
        while (current < hi) {
            recordA.setIndex(current);
            if (colTopsFilter.getBool(recordA)) {
                return;
            }
            current += 1;
        }
    }

    private boolean nextRow() {
        if (current < hi) {
            recordA.setIndex(rows.get(current++));
            return true;
        }
        return nextPage();
    }

    private boolean nextPage() {
        final int columnCount = metadata.getColumnCount();

        PageFrame frame;
        while ((frame = pageFrameCursor.next()) != null) {
            pageFrameIndex += 1;
            recordA.jumpTo(frame, pageFrameIndex);

            final long rowCount = frame.getPartitionHi() - frame.getPartitionLo();

            if (hasColumnTops(frame)) {
                // Use Java filter implementation in case of a page frame with column tops.

                current = 0;
                hi = rowCount;
                seekNextColTopsRow();

                if (current < hi) {
                    next = nextColTopsRow;
                    return true;
                }
                continue;
            }

            // Use compiled filter in case of a dense page frame.

            columns.extend(columnCount);
            columns.clear();
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                final long columnBaseAddress = frame.getPageAddress(columnIndex);
                columns.add(columnBaseAddress);
            }
            // TODO: page frames may be quite large; we may want to break them into smaller subframes
            rows.extend(rowCount);

            current = 0;
            hi = compiledFilter.call(
                    columns.getAddress(),
                    columns.size(),
                    bindVarMemory.getAddress(),
                    bindVarCount,
                    rows.getAddress(),
                    rowCount,
                    0
            );

            if (current < hi) {
                recordA.setIndex(rows.get(current));
                current += 1;
                next = nextRow;
                return true;
            }
        }
        return false;
    }

    private boolean hasColumnTops(PageFrame frame) {
        final int columnCount = metadata.getColumnCount();
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            if (frame.getPageAddress(columnIndex) == 0) {
                return true;
            }
        }
        return false;
    }

    private void writeBindVarFunction(Function function, SqlExecutionContext executionContext) throws SqlException {
        final int columnType = function.getType();
        final int columnTypeTag = ColumnType.tagOf(columnType);
        switch (columnTypeTag) {
            case ColumnType.BOOLEAN:
                bindVarMemory.putLong(function.getBool(null) ? 1 : 0);
                return;
            case ColumnType.BYTE:
                bindVarMemory.putLong(function.getByte(null));
                return;
            case ColumnType.GEOBYTE:
                bindVarMemory.putLong(function.getGeoByte(null));
                return;
            case ColumnType.SHORT:
                bindVarMemory.putLong(function.getShort(null));
                return;
            case ColumnType.GEOSHORT:
                bindVarMemory.putLong(function.getGeoShort(null));
                return;
            case ColumnType.CHAR:
                bindVarMemory.putLong(function.getChar(null));
                return;
            case ColumnType.INT:
                bindVarMemory.putLong(function.getInt(null));
                return;
            case ColumnType.GEOINT:
                bindVarMemory.putLong(function.getGeoInt(null));
                return;
            case ColumnType.SYMBOL:
                assert function instanceof CompiledFilterSymbolBindVariable;
                function.init(this, executionContext);
                bindVarMemory.putLong(function.getInt(null));
                return;
            case ColumnType.FLOAT:
                // compiled filter function will read only the first word
                bindVarMemory.putFloat(function.getFloat(null));
                bindVarMemory.putFloat(Float.NaN);
                return;
            case ColumnType.LONG:
                bindVarMemory.putLong(function.getLong(null));
                return;
            case ColumnType.GEOLONG:
                bindVarMemory.putLong(function.getGeoLong(null));
                return;
            case ColumnType.DATE:
                bindVarMemory.putLong(function.getDate(null));
                return;
            case ColumnType.TIMESTAMP:
                bindVarMemory.putLong(function.getTimestamp(null));
                return;
            case ColumnType.DOUBLE:
                bindVarMemory.putDouble(function.getDouble(null));
                return;
            default:
                throw SqlException.position(0).put("unsupported bind variable type: ").put(ColumnType.nameOf(columnTypeTag));
        }
    }

    public static class PageFrameRecord implements Record {

        private final ByteSequenceView bsview = new ByteSequenceView();
        private final CharSequenceView csview = new CharSequenceView();
        private final CharSequenceView csview2 = new CharSequenceView();
        private final Long256Impl long256A = new Long256Impl();
        private final Long256Impl long256B = new Long256Impl();

        private PageFrameCursor cursor;
        private PageFrame frame;
        private int pageFrameIndex;
        private long index;

        public void jumpTo(PageFrame frame, int pageFrameIndex) {
            this.frame = frame;
            this.pageFrameIndex = pageFrameIndex;
            this.index = frame.getPartitionLo();
        }

        public void setIndex(long index) {
            this.index = index;
        }

        public void of(PageFrameCursor cursor) {
            this.cursor = cursor;
            this.frame = null;
            this.pageFrameIndex = 0;
            this.index = 0;
        }

        @Override
        public long getRowId() {
            return Rows.toRowID(pageFrameIndex, index);
        }

        @Override
        public BinarySequence getBin(int columnIndex) {
            final long dataPageAddress = frame.getPageAddress(columnIndex);
            if (dataPageAddress == 0) {
                return NullColumn.INSTANCE.getBin(0);
            }
            final long indexPageAddress = frame.getIndexPageAddress(columnIndex);
            final long offset = Unsafe.getUnsafe().getLong(indexPageAddress + index * Long.BYTES);
            final long size = frame.getPageSize(columnIndex);
            return getBin(dataPageAddress, offset, size, bsview);
        }

        private BinarySequence getBin(long base, long offset, long size, ByteSequenceView view) {
            final long address = base + offset;
            final long len = Unsafe.getUnsafe().getLong(address);
            if (len != TableUtils.NULL_LEN) {
                if (len + Long.BYTES + offset <= size) {
                    return view.of(address + Long.BYTES, len);
                }
                throw CairoException.instance(0)
                        .put("Bin is outside of file boundary [offset=")
                        .put(offset)
                        .put(", len=")
                        .put(len)
                        .put(", size=")
                        .put(size)
                        .put(']');
            }
            return null;
        }

        @Override
        public long getBinLen(int columnIndex) {
            final long dataPageAddress = frame.getPageAddress(columnIndex);
            if (dataPageAddress == 0) {
                return NullColumn.INSTANCE.getBinLen(0);
            }
            final long indexPageAddress = frame.getIndexPageAddress(columnIndex);
            final long offset = Unsafe.getUnsafe().getLong(indexPageAddress + index * Long.BYTES);
            return Unsafe.getUnsafe().getLong(dataPageAddress + offset);
        }

        @Override
        public boolean getBool(int columnIndex) {
            final long address = frame.getPageAddress(columnIndex);
            if (address == 0) {
                return NullColumn.INSTANCE.getBool(0);
            }
            return Unsafe.getUnsafe().getByte(address + index * Byte.BYTES) == 1;
        }

        @Override
        public byte getByte(int columnIndex) {
            final long address = frame.getPageAddress(columnIndex);
            if (address == 0) {
                return NullColumn.INSTANCE.getByte(0);
            }
            return Unsafe.getUnsafe().getByte(address + index * Byte.BYTES);
        }

        @Override
        public double getDouble(int columnIndex) {
            final long address = frame.getPageAddress(columnIndex);
            if (address == 0) {
                return NullColumn.INSTANCE.getDouble(0);
            }
            return Unsafe.getUnsafe().getDouble(address + index * Double.BYTES);
        }

        @Override
        public float getFloat(int columnIndex) {
            final long address = frame.getPageAddress(columnIndex);
            if (address == 0) {
                return NullColumn.INSTANCE.getFloat(0);
            }
            return Unsafe.getUnsafe().getFloat(address + index * Float.BYTES);
        }

        @Override
        public int getInt(int columnIndex) {
            final long address = frame.getPageAddress(columnIndex);
            if (address == 0) {
                return NullColumn.INSTANCE.getInt(0);
            }
            return Unsafe.getUnsafe().getInt(address + index * Integer.BYTES);
        }

        @Override
        public long getLong(int columnIndex) {
            final long address = frame.getPageAddress(columnIndex);
            if (address == 0) {
                return NullColumn.INSTANCE.getLong(0);
            }
            return Unsafe.getUnsafe().getLong(address + index * Long.BYTES);
        }

        @Override
        public short getShort(int columnIndex) {
            final long address = frame.getPageAddress(columnIndex);
            if (address == 0) {
                return NullColumn.INSTANCE.getShort(0);
            }
            return Unsafe.getUnsafe().getShort(address + index * Short.BYTES);
        }

        @Override
        public char getChar(int columnIndex) {
            final long address = frame.getPageAddress(columnIndex);
            if (address == 0) {
                return NullColumn.INSTANCE.getChar(0);
            }
            return Unsafe.getUnsafe().getChar(address + index * Character.BYTES);
        }

        @Override
        public CharSequence getStr(int columnIndex) {
            final long dataPageAddress = frame.getPageAddress(columnIndex);
            if (dataPageAddress == 0) {
                return NullColumn.INSTANCE.getStr(0);
            }
            final long indexPageAddress = frame.getIndexPageAddress(columnIndex);
            final long offset = Unsafe.getUnsafe().getLong(indexPageAddress + index * Long.BYTES);
            final long size = frame.getPageSize(columnIndex);
            return getStr(dataPageAddress, offset, size, csview);
        }

        private CharSequence getStr(long base, long offset, long size, CharSequenceView view) {
            final long address = base + offset;
            final int len = Unsafe.getUnsafe().getInt(address);
            if (len != TableUtils.NULL_LEN) {
                if (len + 4 + offset <= size) {
                    return view.of(address + Vm.STRING_LENGTH_BYTES, len);
                }
                throw CairoException.instance(0)
                        .put("String is outside of file boundary [offset=")
                        .put(offset)
                        .put(", len=")
                        .put(len)
                        .put(", size=")
                        .put(size)
                        .put(']');
            }
            return null;
        }

        @Override
        public int getStrLen(int columnIndex) {
            final long dataPageAddress = frame.getPageAddress(columnIndex);
            if (dataPageAddress == 0) {
                return NullColumn.INSTANCE.getStrLen(0);
            }
            final long indexPageAddress = frame.getIndexPageAddress(columnIndex);
            final long offset = Unsafe.getUnsafe().getLong(indexPageAddress + index * Long.BYTES);
            return Unsafe.getUnsafe().getInt(dataPageAddress + offset);
        }

        @Override
        public CharSequence getStrB(int columnIndex) {
            final long dataPageAddress = frame.getPageAddress(columnIndex);
            if (dataPageAddress == 0) {
                return NullColumn.INSTANCE.getStr2(0);
            }
            final long indexPageAddress = frame.getIndexPageAddress(columnIndex);
            final long offset = Unsafe.getUnsafe().getLong(indexPageAddress + index * Long.BYTES);
            final long size = frame.getPageSize(columnIndex);
            return getStr(dataPageAddress, offset, size, csview2);
        }

        @Override
        public void getLong256(int columnIndex, CharSink sink) {
            final long address = frame.getPageAddress(columnIndex);
            if (address == 0) {
                NullColumn.INSTANCE.getLong256(0, sink);
                return;
            }
            getLong256(address + index * Long256.BYTES, sink);
        }

        void getLong256(long offset, CharSink sink) {
            final long addr = offset + Long.BYTES * 4;
            final long a, b, c, d;
            a = Unsafe.getUnsafe().getLong(addr - Long.BYTES * 4);
            b = Unsafe.getUnsafe().getLong(addr - Long.BYTES * 3);
            c = Unsafe.getUnsafe().getLong(addr - Long.BYTES * 2);
            d = Unsafe.getUnsafe().getLong(addr - Long.BYTES);
            Numbers.appendLong256(a, b, c, d, sink);
        }

        @Override
        public Long256 getLong256A(int columnIndex) {
            getLong256(columnIndex, long256A);
            return long256A;
        }

        @Override
        public Long256 getLong256B(int columnIndex) {
            getLong256(columnIndex, long256B);
            return long256B;
        }

        void getLong256(int columnIndex, Long256Acceptor sink) {
            final long columnAddress = frame.getPageAddress(columnIndex);
            if (columnAddress == 0) {
                NullColumn.INSTANCE.getLong256(0, sink);
                return;
            }
            final long addr = columnAddress + index * Long256.BYTES  + Long.BYTES * 4;
            sink.setAll(
                    Unsafe.getUnsafe().getLong(addr - Long.BYTES * 4),
                    Unsafe.getUnsafe().getLong(addr - Long.BYTES * 3),
                    Unsafe.getUnsafe().getLong(addr - Long.BYTES * 2),
                    Unsafe.getUnsafe().getLong(addr - Long.BYTES)
            );
        }

        @Override
        public CharSequence getSym(int columnIndex) {
            final long address = frame.getPageAddress(columnIndex);
            int key = NullColumn.INSTANCE.getInt(0);
            if (address != 0) {
                key = Unsafe.getUnsafe().getInt(address + index * Integer.BYTES);
            }
            return cursor.getSymbolMapReader(columnIndex).valueOf(key);
        }

        @Override
        public CharSequence getSymB(int columnIndex) {
            final long address = frame.getPageAddress(columnIndex);
            final int key = Unsafe.getUnsafe().getInt(address + index * Integer.BYTES);
            return cursor.getSymbolMapReader(columnIndex).valueBOf(key);
        }

        @Override
        public byte getGeoByte(int columnIndex) {
            final long address = frame.getPageAddress(columnIndex);
            if (address == 0) {
                return NullColumn.INSTANCE.getByte(0);
            }
            return Unsafe.getUnsafe().getByte(address + index * Byte.BYTES);
        }

        @Override
        public short getGeoShort(int columnIndex) {
            final long address = frame.getPageAddress(columnIndex);
            if (address == 0) {
                return NullColumn.INSTANCE.getShort(0);
            }
            return Unsafe.getUnsafe().getShort(address + index * Short.BYTES);
        }

        @Override
        public int getGeoInt(int columnIndex) {
            final long address = frame.getPageAddress(columnIndex);
            if (address == 0) {
                return NullColumn.INSTANCE.getInt(0);
            }
            return Unsafe.getUnsafe().getInt(address + index * Integer.BYTES);
        }

        @Override
        public long getGeoLong(int columnIndex) {
            final long address = frame.getPageAddress(columnIndex);
            if (address == 0) {
                return NullColumn.INSTANCE.getLong(0);
            }
            return Unsafe.getUnsafe().getLong(address + index * Long.BYTES);
        }

        private static class ByteSequenceView implements BinarySequence {
            private long address;
            private long len = -1;

            @Override
            public byte byteAt(long index) {
                return Unsafe.getUnsafe().getByte(address + index);
            }

            @Override
            public void copyTo(long address, final long start, final long length) {
                long bytesRemaining = Math.min(length, this.len - start);
                long addr = this.address + start;
                Vect.memcpy(address, addr, bytesRemaining);
            }

            @Override
            public long length() {
                return len;
            }

            ByteSequenceView of(long address, long len) {
                this.address = address;
                this.len = len;
                return this;
            }
        }

        private static class CharSequenceView extends AbstractCharSequence {
            private int len;
            private long address;

            @Override
            public int length() {
                return len;
            }

            @Override
            public char charAt(int index) {
                return Unsafe.getUnsafe().getChar(address + index * 2L);
            }

            CharSequenceView of(long address, int len) {
                this.address = address;
                this.len = len;
                return this;
            }
        }

    }
}
