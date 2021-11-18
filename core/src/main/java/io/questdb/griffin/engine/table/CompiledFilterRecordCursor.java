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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryAR;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.jit.FiltersCompiler;
import io.questdb.std.*;
import io.questdb.std.str.AbstractCharSequence;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

import java.util.function.BooleanSupplier;

class CompiledFilterRecordCursor implements RecordCursor {

    private final PageFrameRecord recordA;

    private PageFrameCursor pageFrameCursor;
    private RecordMetadata metadata;

    private DirectLongList rows;
    private DirectLongList columns;

    private long hi;
    private long current;

    private BooleanSupplier next;
    private final BooleanSupplier nextRow = this::nextRow;
    private final BooleanSupplier nextPage = this::nextPage;
    private final long filterFnAddress;

    public CompiledFilterRecordCursor(@NotNull IntList columnIndexes, MemoryAR filter) {
        this.recordA = new PageFrameRecord(columnIndexes);
        //todo: what if filter changed outside?
        final long filterSize = filter.getAppendOffset();
        filter.jumpTo(0);

        final long filterAddr = filter.getPageAddress(0);
        //todo: error reporting
        this.filterFnAddress = FiltersCompiler.compileFunction(filterAddr, filterSize, 0);
    }

    @Override
    public void close() {
        pageFrameCursor.close();
        FiltersCompiler.freeFunction(filterFnAddress);
    }

    @Override
    public Record getRecord() {
        return recordA;
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
    public void toTop() {
        pageFrameCursor.toTop();
        next = nextPage;
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public Record getRecordB() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void recordAt(Record record, long rowId) {
        throw new UnsupportedOperationException();
    }

    void of(RecordCursorFactory factory, DirectLongList rows, DirectLongList columns, SqlExecutionContext executionContext) throws SqlException {
        this.rows = rows;
        this.columns = columns;
        this.pageFrameCursor = factory.getPageFrameCursor(executionContext);
        this.metadata = factory.getMetadata();
        this.next = nextPage;
    }

    private boolean nextRow() {
        if (current < hi) {
            recordA.jumpTo(rows.get(current++));
            return true;
        }
        return nextPage();
    }

    private boolean nextPage() {
        PageFrame frame;
        while ((frame = pageFrameCursor.next()) != null) {
            recordA.of(frame, pageFrameCursor);
            int sz = metadata.getColumnCount();
            columns.extend(sz);
            columns.clear();
            for (int columnIndex = 0; columnIndex < sz; columnIndex++) {
                final long columnBaseAddress = frame.getPageAddress(columnIndex);
                columns.add(columnBaseAddress);
            }
            // TODO try to reuse
            rows.extend(frame.getPartitionHi());
            this.current = 0;
            this.hi = FiltersCompiler.callFunction(
                    filterFnAddress,
                    columns.getAddress(),
                    columns.size(),
                    rows.getAddress(),
                    frame.getPartitionHi(),
                    frame.getPartitionLo());

            if (current < hi) {
                recordA.jumpTo(rows.get(current++));
                next = nextRow;
                return true;
            }
        }
        return false;
    }

    public static class PageFrameRecord implements Record {
        private final ByteSequenceView bsview = new ByteSequenceView();
        private final CharSequenceView csview = new CharSequenceView();
        private final CharSequenceView csview2 = new CharSequenceView();
        private final Long256Impl long256A = new Long256Impl();
        private final Long256Impl long256B = new Long256Impl();

        private PageFrameCursor cursor;
        private PageFrame frame;
        private final IntList columnIndexes;
        private long index = 0;

        public PageFrameRecord(@NotNull IntList columnIndexes) {
            this.columnIndexes = columnIndexes;
        }

        public long getIndex() {
            return index;
        }

        public void setIndex(long index) {
            this.index = index;
        }

        public void incrementIndex() {
            index++;
        }

        public void jumpTo(long index) {
            this.index = index;
        }

        public void of(PageFrame frame, PageFrameCursor cursor) {
            this.cursor = cursor;
            this.frame = frame;
            this.index = 0;
        }

        @Override
        public BinarySequence getBin(int columnIndex) {
            final int idx = getColumnIndex(columnIndex);
            final long indexPageAddress = frame.getIndexPageAddress(idx);
            final long offset = Unsafe.getUnsafe().getLong(indexPageAddress + index * Long.BYTES);
            final long dataPageAddress = frame.getPageAddress(idx);
            final long size = frame.getPageSize(idx);
            return getBin(dataPageAddress, offset, size, bsview);
        }

        @Override
        public long getBinLen(int columnIndex) {
            final int idx = getColumnIndex(columnIndex);
            final long indexPageAddress = frame.getIndexPageAddress(idx);
            final long offset = Unsafe.getUnsafe().getLong(indexPageAddress + index * Long.BYTES);
            final long dataPageAddress = frame.getPageAddress(idx);
            return Unsafe.getUnsafe().getLong(dataPageAddress + offset);
        }

        @Override
        public boolean getBool(int columnIndex) {
            final long address = getColumnAddress(columnIndex);
            return Unsafe.getUnsafe().getByte(address + index * Byte.BYTES) == 1;
        }

        @Override
        public byte getByte(int columnIndex) {
            final long address = getColumnAddress(columnIndex);
            return Unsafe.getUnsafe().getByte(address + index * Byte.BYTES);
        }

        @Override
        public double getDouble(int columnIndex) {
            final long address = getColumnAddress(columnIndex);
            return Unsafe.getUnsafe().getDouble(address + index * Double.BYTES);
        }

        @Override
        public float getFloat(int columnIndex) {
            final long address = getColumnAddress(columnIndex);
            return Unsafe.getUnsafe().getFloat(address + index * Float.BYTES);
        }

        @Override
        public int getInt(int columnIndex) {
            final long address = getColumnAddress(columnIndex);
            return Unsafe.getUnsafe().getInt(address + index * Integer.BYTES);
        }

        @Override
        public long getRowId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLong(int columnIndex) {
            final long address = getColumnAddress(columnIndex);
            return Unsafe.getUnsafe().getLong(address + index * Long.BYTES);
        }

        @Override
        public short getShort(int columnIndex) {
            final long address = getColumnAddress(columnIndex);
            return Unsafe.getUnsafe().getShort(address + index * Short.BYTES);
        }

        @Override
        public char getChar(int columnIndex) {
            final long address = getColumnAddress(columnIndex);
            return Unsafe.getUnsafe().getChar(address + index * Character.BYTES);
        }

        @Override
        public CharSequence getStr(int columnIndex) {
            final int idx = getColumnIndex(columnIndex);
            final long indexPageAddress = frame.getIndexPageAddress(idx);
            final long offset = Unsafe.getUnsafe().getLong(indexPageAddress + index * Long.BYTES);
            final long dataPageAddress = frame.getPageAddress(idx);
            final long size = frame.getPageSize(idx);
            return getStr(dataPageAddress, offset, size, csview);
        }

        @Override
        public int getStrLen(int columnIndex) {
            final int idx = getColumnIndex(columnIndex);
            final long indexPageAddress = frame.getIndexPageAddress(idx);
            final long offset = Unsafe.getUnsafe().getLong(indexPageAddress + index * Long.BYTES);
            final long dataPageAddress = frame.getPageAddress(idx);
            return Unsafe.getUnsafe().getInt(dataPageAddress + offset);
        }

        @Override
        public CharSequence getStrB(int columnIndex) {
            final int idx = getColumnIndex(columnIndex);
            final long indexPageAddress = frame.getIndexPageAddress(idx);
            final long offset = Unsafe.getUnsafe().getLong(indexPageAddress + index * Long.BYTES);
            final long dataPageAddress = frame.getPageAddress(idx);
            final long size = frame.getPageSize(idx);
            return getStr(dataPageAddress, offset, size, csview2);
        }

        @Override
        public void getLong256(int columnIndex, CharSink sink) {
            final long address = getColumnAddress(columnIndex);
            getLong256(address + index * Long256.BYTES, sink);
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

        @Override
        public CharSequence getSym(int columnIndex) {
            final long address = getColumnAddress(columnIndex);
            final int key = Unsafe.getUnsafe().getInt(address + index * Integer.BYTES);
            return cursor.getSymbolMapReader(getColumnIndex(columnIndex)).valueOf(key);
        }

        @Override
        public CharSequence getSymB(int columnIndex) {
            final long address = getColumnAddress(columnIndex);
            final int key = Unsafe.getUnsafe().getInt(address + index * Integer.BYTES);
            return cursor.getSymbolMapReader(getColumnIndex(columnIndex)).valueBOf(key);
        }

        @Override
        public byte getGeoByte(int columnIndex) {
            final long address = getColumnAddress(columnIndex);
            return Unsafe.getUnsafe().getByte(address + index * Byte.BYTES);
        }

        @Override
        public short getGeoShort(int columnIndex) {
            final long address = getColumnAddress(columnIndex);
            return Unsafe.getUnsafe().getShort(address + index * Short.BYTES);
        }

        @Override
        public int getGeoInt(int columnIndex) {
            final long address = getColumnAddress(columnIndex);
            return Unsafe.getUnsafe().getInt(address + index * Integer.BYTES);
        }

        @Override
        public long getGeoLong(int columnIndex) {
            final long address = getColumnAddress(columnIndex);
            return Unsafe.getUnsafe().getLong(address + index * Long.BYTES);
        }

        private long getColumnAddress(int columnIndex) {
            final int idx = getColumnIndex(columnIndex);
            return frame.getPageAddress(idx);
        }

        private int getColumnIndex(int columnIndex) {
            return columnIndexes.getQuick(columnIndex);
        }

        void getLong256(long addr, CharSink sink) {
            final long a, b, c, d;
            a = Unsafe.getUnsafe().getLong(addr - Long.BYTES * 4);
            b = Unsafe.getUnsafe().getLong(addr - Long.BYTES * 3);
            c = Unsafe.getUnsafe().getLong(addr - Long.BYTES * 2);
            d = Unsafe.getUnsafe().getLong(addr - Long.BYTES);
            Numbers.appendLong256(a, b, c, d, sink);
        }

        void getLong256(int columnIndex, Long256Acceptor sink) {
            long columnAddress = getColumnAddress(columnIndex);
            long addr = columnAddress + index * Long.BYTES * 4;
            sink.setAll(
                    Unsafe.getUnsafe().getLong(addr - Long.BYTES * 4),
                    Unsafe.getUnsafe().getLong(addr - Long.BYTES * 3),
                    Unsafe.getUnsafe().getLong(addr - Long.BYTES * 2),
                    Unsafe.getUnsafe().getLong(addr - Long.BYTES)
            );
        }

        BinarySequence getBin(long base, long offset, long size, ByteSequenceView view) {
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

        CharSequence getStr(long base, long offset, long size, CharSequenceView view) {
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

        static class ByteSequenceView implements BinarySequence {
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

        static class CharSequenceView extends AbstractCharSequence {
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
