/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cairo.wal;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.griffin.SqlException;
import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.DirectByteSequenceView;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjectPool;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;

import static io.questdb.cairo.wal.WalTxnType.*;
import static io.questdb.cairo.wal.WalUtils.*;

public class WalEventCursor {
    public static final long END_OF_EVENTS = -1L;
    private static final int DEDUP_MODE_OFFSET = Byte.BYTES;
    private static final int REPLACE_RANGE_HI_OFFSET = DEDUP_MODE_OFFSET + Long.BYTES;
    private static final int REPLACE_RANGE_LO_OFFSET = REPLACE_RANGE_HI_OFFSET + Long.BYTES;
    private static final int REPLACE_RANGE_EXTRA_OFFSET = REPLACE_RANGE_LO_OFFSET + Long.BYTES;
    private static final int DEDUP_FOOTER_SIZE = REPLACE_RANGE_EXTRA_OFFSET;
    private final DataInfo dataInfo = new DataInfo();
    private final MemoryCMR eventMem;
    private final MatViewDataInfo mvDataInfo = new MatViewDataInfo();
    private final MatViewInvalidationInfo mvInvalidationInfo = new MatViewInvalidationInfo();
    private final SqlInfo sqlInfo = new SqlInfo();
    private long memSize;
    private long nextOffset = Integer.BYTES;
    private long offset = Integer.BYTES; // skip wal meta version
    private long txn = END_OF_EVENTS;
    private byte type = NONE;

    public WalEventCursor(MemoryCMR eventMem) {
        this.eventMem = eventMem;
    }

    public void drain() {
        long o = offset;
        while (true) {
            if (memSize >= o + Integer.BYTES) {
                final int value = eventMem.getInt(o);
                o += Integer.BYTES;
                if (value == SymbolMapDiffImpl.END_OF_SYMBOL_ENTRIES) {
                    offset = o;
                    break;
                }

                final int strLength = eventMem.getStrLen(o);
                final long storageLength = Vm.getStorageLength(strLength);
                o += storageLength;
            } else {
                throw CairoException.critical(0).put("WAL event file is too small, size=").put(memSize)
                        .put(", required=").put(o + Integer.BYTES);
            }
        }
    }

    public DataInfo getDataInfo() {
        if (!WalTxnType.isDataType(type)) {
            throw CairoException.critical(CairoException.ILLEGAL_OPERATION).put("WAL event type is not DATA, type=").put(type);
        }
        return (type == DATA) ? dataInfo : mvDataInfo;
    }

    public MatViewDataInfo getMatViewDataInfo() {
        if (type != MAT_VIEW_DATA) {
            throw CairoException.critical(CairoException.ILLEGAL_OPERATION).put("WAL event type is not MAT_VIEW_DATA, type=").put(type);
        }
        return mvDataInfo;
    }

    public MatViewInvalidationInfo getMatViewInvalidationInfo() {
        if (type != MAT_VIEW_INVALIDATE) {
            throw CairoException.critical(CairoException.ILLEGAL_OPERATION).put("WAL event type is not MAT_VIEW_INVALIDATION, type=").put(type);
        }
        return mvInvalidationInfo;
    }

    public SqlInfo getSqlInfo() {
        if (type != SQL) {
            throw CairoException.critical(CairoException.ILLEGAL_OPERATION).put("WAL event type is not SQL, type=").put(type);
        }
        return sqlInfo;
    }

    public long getTxn() {
        return txn;
    }

    public byte getType() {
        return type;
    }

    public boolean hasNext() {
        offset = nextOffset;
        int length = readInt();
        if (length < 1) {
            // EOF
            return false;
        }
        nextOffset = length + nextOffset;

        if (memSize < nextOffset + Integer.BYTES) {
            eventMem.extend(nextOffset + Integer.BYTES);
            memSize = eventMem.size();
        }
        txn = readLong();
        if (txn == END_OF_EVENTS) {
            return false;
        }
        readRecord();
        return true;
    }

    public void reset() {
        memSize = eventMem.size();
        nextOffset = WALE_HEADER_SIZE; // skip wal meta version
        txn = END_OF_EVENTS;
        type = WalTxnType.NONE;
    }

    private void checkMemSize(long requiredBytes) {
        if (memSize < offset + requiredBytes) {
            throw CairoException.critical(0).put("WAL event file is too small, size=").put(memSize)
                    .put(", required=").put(offset + requiredBytes);
        }
    }

    private ArrayView readArray(BorrowedArray array) {
        checkMemSize(Long.BYTES);
        long totalSize = eventMem.getLong(offset);
        if (totalSize < 0) {
            totalSize = 0;
        }
        checkMemSize(totalSize + Long.BYTES);
        eventMem.getArray(offset, array);
        offset += Long.BYTES + totalSize;
        return array;
    }

    private BinarySequence readBin(DirectByteSequenceView view) {
        checkMemSize(Long.BYTES);
        final long binLength = eventMem.getBinLen(offset);
        checkMemSize(binLength);
        view.of((DirectByteSequenceView) eventMem.getBin(offset));
        offset += binLength + Long.BYTES;
        return view;
    }

    private boolean readBool() {
        checkMemSize(Byte.BYTES);
        final boolean value = eventMem.getBool(offset);
        offset += Byte.BYTES;
        return value;
    }

    private byte readByte() {
        checkMemSize(Byte.BYTES);
        final byte value = eventMem.getByte(offset);
        offset += Byte.BYTES;
        return value;
    }

    private char readChar() {
        checkMemSize(Character.BYTES);
        final char value = eventMem.getChar(offset);
        offset += Character.BYTES;
        return value;
    }

    private double readDouble() {
        checkMemSize(Double.BYTES);
        final double value = eventMem.getDouble(offset);
        offset += Double.BYTES;
        return value;
    }

    private float readFloat() {
        checkMemSize(Float.BYTES);
        final float value = eventMem.getFloat(offset);
        offset += Float.BYTES;
        return value;
    }

    private int readInt() {
        checkMemSize(Integer.BYTES);
        final int value = eventMem.getInt(offset);
        offset += Integer.BYTES;
        return value;
    }

    private long readLong() {
        checkMemSize(Long.BYTES);
        final long value = eventMem.getLong(offset);
        offset += Long.BYTES;
        return value;
    }

    private void readRecord() {
        type = readByte();
        switch (type) {
            case DATA:
                dataInfo.read();
                break;
            case MAT_VIEW_DATA:
                mvDataInfo.read();
                break;
            case SQL:
                sqlInfo.read();
                break;
            case TRUNCATE:
                break;
            case MAT_VIEW_INVALIDATE:
                mvInvalidationInfo.read();
                break;
            default:
                throw CairoException.critical(CairoException.METADATA_VALIDATION).put("Unsupported WAL event type: ").put(type);
        }
    }

    private short readShort() {
        checkMemSize(Short.BYTES);
        final short value = eventMem.getShort(offset);
        offset += Short.BYTES;
        return value;
    }

    private CharSequence readStr() {
        checkMemSize(Integer.BYTES);
        final int strLength = eventMem.getStrLen(offset);
        final long storageLength = strLength > 0 ? Vm.getStorageLength(strLength) : Integer.BYTES;

        checkMemSize(storageLength);
        final CharSequence value = strLength >= 0 ? eventMem.getStrA(offset) : null;
        offset += storageLength;
        return value;
    }

    private long readStrOffset() {
        checkMemSize(Integer.BYTES);
        final int strLength = eventMem.getStrLen(offset);
        final long storageLength = strLength > 0 ? Vm.getStorageLength(strLength) : Integer.BYTES;

        checkMemSize(storageLength);
        offset += storageLength;
        return offset - storageLength;
    }

    private Utf8Sequence readVarchar() {
        Utf8Sequence seq = VarcharTypeDriver.getPlainValue(eventMem, offset);
        if (seq == null) {
            offset += Integer.BYTES;
            return null;
        } else {
            offset += seq.size() + Integer.BYTES;
            return seq;
        }
    }

    void openOffset(long offset) {
        reset();
        if (offset > 0) {
            this.offset = offset;
            int size = readInt();
            this.nextOffset = offset + size;
            this.txn = readLong();
            this.memSize = eventMem.size();

            readRecord();
        }
    }

    SymbolMapDiff readNextSymbolMapDiff(SymbolMapDiffImpl symbolMapDiff) {
        final int columnIndex = readInt();
        if (columnIndex == SymbolMapDiffImpl.END_OF_SYMBOL_DIFFS) {
            return null;
        }
        final boolean nullFlag = readBool();
        final int cleanTableSymbolCount = readInt();
        final int size = readInt();

        symbolMapDiff.of(columnIndex, cleanTableSymbolCount, size, nullFlag);
        return symbolMapDiff;
    }

    SymbolMapDiffImpl.Entry readNextSymbolMapDiffEntry(SymbolMapDiffImpl.Entry entry) {
        final int key = readInt();
        if (key == SymbolMapDiffImpl.END_OF_SYMBOL_ENTRIES) {
            entry.clear();
            return null;
        }
        final long symbolOffset = readStrOffset();
        entry.of(key, symbolOffset, eventMem);
        return entry;
    }

    public class DataInfo implements SymbolMapDiffCursor {
        private final SymbolMapDiffImpl symbolMapDiff = new SymbolMapDiffImpl(WalEventCursor.this);
        // extra field, for now used only in mat views
        protected long replaceRangeExtra;
        private byte dedupMode;
        private long endRowID;
        private long maxTimestamp;
        private long minTimestamp;
        private boolean outOfOrder;
        private long replaceRangeTsHi;
        private long replaceRangeTsLow;
        private long startRowID;

        public byte getDedupMode() {
            return dedupMode;
        }

        public long getEndRowID() {
            return endRowID;
        }

        public long getMaxTimestamp() {
            return maxTimestamp;
        }

        public long getMinTimestamp() {
            return minTimestamp;
        }

        public long getReplaceRangeTsHi() {
            return replaceRangeTsHi;
        }

        public long getReplaceRangeTsLow() {
            return replaceRangeTsLow;
        }

        public long getStartRowID() {
            return startRowID;
        }

        public boolean isOutOfOrder() {
            return outOfOrder;
        }

        @Override
        public SymbolMapDiff nextSymbolMapDiff() {
            return readNextSymbolMapDiff(symbolMapDiff);
        }

        protected void read() {
            startRowID = readLong();
            endRowID = readLong();
            minTimestamp = readLong();
            maxTimestamp = readLong();
            outOfOrder = readBool();

            // Read the footer that may contains replace range timestamps and dedup mode
            // The footer is not written when the dedup mode is default
            // Format:
            // [replaceRangeTsLow:long, replaceRangeTsHi:long, dedupMode:byte]
            // Length of this footer is 2 * Long.BYTES + Byte.BYTES
            dedupMode = WAL_DEDUP_MODE_DEFAULT;
            replaceRangeTsLow = 0;
            replaceRangeTsHi = 0;

            if (nextOffset - offset >= Integer.BYTES + DEDUP_FOOTER_SIZE) {
                // This is big enough to contain the footer.
                // But it can be still populated with symbol map values instead of the footer.
                // Check that the last symbol map diff entry contains the END of symbol diffs marker.

                // Read column index before the footer.
                int symbolColIndex = eventMem.getInt(nextOffset - (Integer.BYTES + DEDUP_FOOTER_SIZE));

                if (symbolColIndex == SymbolMapDiffImpl.END_OF_SYMBOL_DIFFS) {
                    dedupMode = eventMem.getByte(nextOffset - DEDUP_MODE_OFFSET);
                    if (dedupMode >= 0 && dedupMode <= WAL_DEDUP_MODE_MAX) {
                        replaceRangeExtra = eventMem.getLong(nextOffset - REPLACE_RANGE_EXTRA_OFFSET);
                        replaceRangeTsLow = eventMem.getLong(nextOffset - REPLACE_RANGE_LO_OFFSET);
                        replaceRangeTsHi = eventMem.getLong(nextOffset - REPLACE_RANGE_HI_OFFSET);
                    } else {
                        // This WAL record does not have dedup mode recognised, clean unrecognised mode value
                        dedupMode = WAL_DEDUP_MODE_DEFAULT;
                    }
                }
            }
        }
    }

    public class MatViewDataInfo extends DataInfo {
        private long lastRefreshBaseTableTxn;
        private long lastRefreshTimestampUs;

        public long getLastPeriodHi() {
            return replaceRangeExtra;
        }

        public long getLastRefreshBaseTableTxn() {
            return lastRefreshBaseTableTxn;
        }

        public long getLastRefreshTimestampUs() {
            return lastRefreshTimestampUs;
        }

        @Override
        protected void read() {
            super.read();
            // read the extra fields in the fixed part
            // symbol map will start after this
            lastRefreshBaseTableTxn = readLong();
            lastRefreshTimestampUs = readLong();
        }
    }

    public class MatViewInvalidationInfo {
        private final StringSink error = new StringSink();
        private final LongList refreshIntervals = new LongList();
        private boolean invalid;
        private long lastPeriodHi;
        private long lastRefreshBaseTableTxn;
        private long lastRefreshTimestampUs;
        private long refreshIntervalsBaseTxn;

        public CharSequence getInvalidationReason() {
            return error;
        }

        public long getLastPeriodHi() {
            return lastPeriodHi;
        }

        public long getLastRefreshBaseTableTxn() {
            return lastRefreshBaseTableTxn;
        }

        public long getLastRefreshTimestampUs() {
            return lastRefreshTimestampUs;
        }

        public LongList getRefreshIntervals() {
            return refreshIntervals;
        }

        public long getRefreshIntervalsBaseTxn() {
            return refreshIntervalsBaseTxn;
        }

        public boolean isInvalid() {
            return invalid;
        }

        private void read() {
            lastRefreshBaseTableTxn = readLong();
            lastRefreshTimestampUs = readLong();
            invalid = readBool();
            error.clear();
            error.put(readStr());

            if (nextOffset - offset >= Long.BYTES) {
                lastPeriodHi = readLong();
            } else {
                lastPeriodHi = Numbers.LONG_NULL;
            }

            refreshIntervals.clear();
            if (nextOffset - offset >= Long.BYTES + Integer.BYTES) {
                refreshIntervalsBaseTxn = readLong();
                final int intervalsLen = readInt();
                for (int i = 0; i < intervalsLen; i++) {
                    refreshIntervals.add(readLong());
                }
            } else {
                refreshIntervalsBaseTxn = -1;
            }
        }
    }

    public class SqlInfo {
        private final ObjectPool<BorrowedArray> arrayViewPool = new ObjectPool<>(BorrowedArray::new, 1);
        private final ObjectPool<DirectByteSequenceView> byteViewPool = new ObjectPool<>(DirectByteSequenceView::new, 1);
        private final StringSink sql = new StringSink();
        private int cmdType;
        private long rndSeed0;
        private long rndSeed1;

        public int getCmdType() {
            return cmdType;
        }

        public long getRndSeed0() {
            return rndSeed0;
        }

        public long getRndSeed1() {
            return rndSeed1;
        }

        public CharSequence getSql() {
            return sql;
        }

        public void populateBindVariableService(BindVariableService bindVariableService) {
            bindVariableService.clear();
            try {
                populateIndexedVariables(bindVariableService);
                populateNamedVariables(bindVariableService);
            } catch (SqlException e) {
                throw CairoException.critical(0).put(e.getMessage());
            }
        }

        private void populateIndexedVariables(BindVariableService bindVariableService) throws SqlException {
            final int count = readInt();
            for (int i = 0; i < count; i++) {
                final int type = readInt();
                switch (ColumnType.tagOf(type)) {
                    case ColumnType.BOOLEAN:
                        bindVariableService.setBoolean(i, readBool());
                        break;
                    case ColumnType.BYTE:
                        bindVariableService.setByte(i, readByte());
                        break;
                    case ColumnType.SHORT:
                        bindVariableService.setShort(i, readShort());
                        break;
                    case ColumnType.CHAR:
                        bindVariableService.setChar(i, readChar());
                        break;
                    case ColumnType.INT:
                    case ColumnType.IPv4:
                        bindVariableService.setInt(i, readInt());
                        break;
                    case ColumnType.FLOAT:
                        bindVariableService.setFloat(i, readFloat());
                        break;
                    case ColumnType.LONG:
                    case ColumnType.DATE:
                    case ColumnType.TIMESTAMP:
                        bindVariableService.setLong(i, readLong());
                        break;
                    case ColumnType.DOUBLE:
                        bindVariableService.setDouble(i, readDouble());
                        break;
                    case ColumnType.STRING:
                        bindVariableService.setStr(i, readStr());
                        break;
                    case ColumnType.VARCHAR:
                        bindVariableService.setVarchar(i, readVarchar());
                        break;
                    case ColumnType.BINARY:
                        bindVariableService.setBin(i, readBin(byteViewPool.next()));
                        break;
                    case ColumnType.GEOBYTE:
                        bindVariableService.setGeoHash(i, readByte(), type);
                        break;
                    case ColumnType.GEOSHORT:
                        bindVariableService.setGeoHash(i, readShort(), type);
                        break;
                    case ColumnType.GEOINT:
                        bindVariableService.setGeoHash(i, readInt(), type);
                        break;
                    case ColumnType.GEOLONG:
                        bindVariableService.setGeoHash(i, readLong(), type);
                        break;
                    case ColumnType.UUID:
                        bindVariableService.setUuid(i, readLong(), readLong());
                        break;
                    case ColumnType.ARRAY:
                        // Multiple arrayView objects might be bind to variables, and in `ArrayBindVariable`,
                        // arrayView does not clone its meta information, so `arrayViewPool` is needed.
                        // Same as `setBin`
                        bindVariableService.setArray(i, readArray(arrayViewPool.next()));
                        break;
                    case ColumnType.DECIMAL8:
                        byte decimal8 = readByte();
                        long s = decimal8 < 0 ? -1 : 0;
                        bindVariableService.setDecimal(i, s, s, s, decimal8, type);
                        break;
                    case ColumnType.DECIMAL16:
                        short decimal16 = readShort();
                        s = decimal16 < 0 ? -1 : 0;
                        bindVariableService.setDecimal(i, s, s, s, decimal16, type);
                        break;
                    case ColumnType.DECIMAL32:
                        int decimal32 = readInt();
                        s = decimal32 < 0 ? -1 : 0;
                        bindVariableService.setDecimal(i, s, s, s, decimal32, type);
                        break;
                    case ColumnType.DECIMAL64:
                        long decimal64 = readLong();
                        s = decimal64 < 0 ? -1 : 0;
                        bindVariableService.setDecimal(i, s, s, s, decimal64, type);
                        break;
                    case ColumnType.DECIMAL128:
                        long hi = readLong();
                        long lo = readLong();
                        s = hi < 0 ? -1 : 0;
                        bindVariableService.setDecimal(i, s, s, hi, lo, type);
                        break;
                    case ColumnType.DECIMAL256:
                        bindVariableService.setDecimal(i, readLong(), readLong(), readLong(), readLong(), type);
                        break;
                    default:
                        throw new UnsupportedOperationException("unsupported column type: " + ColumnType.nameOf(type));
                }
            }
        }

        private void populateNamedVariables(BindVariableService bindVariableService) throws SqlException {
            final int count = readInt();
            for (int i = 0; i < count; i++) {
                // garbage, string intern?
                final CharSequence name = Chars.toString(readStr());
                final int type = readInt();
                switch (ColumnType.tagOf(type)) {
                    case ColumnType.BOOLEAN:
                        bindVariableService.setBoolean(name, readBool());
                        break;
                    case ColumnType.BYTE:
                        bindVariableService.setByte(name, readByte());
                        break;
                    case ColumnType.SHORT:
                        bindVariableService.setShort(name, readShort());
                        break;
                    case ColumnType.CHAR:
                        bindVariableService.setChar(name, readChar());
                        break;
                    case ColumnType.INT:
                    case ColumnType.IPv4:
                        bindVariableService.setInt(name, readInt());
                        break;
                    case ColumnType.FLOAT:
                        bindVariableService.setFloat(name, readFloat());
                        break;
                    case ColumnType.LONG:
                    case ColumnType.DATE:
                    case ColumnType.TIMESTAMP:
                        bindVariableService.setLong(name, readLong());
                        break;
                    case ColumnType.DOUBLE:
                        bindVariableService.setDouble(name, readDouble());
                        break;
                    case ColumnType.STRING:
                        bindVariableService.setStr(name, readStr());
                        break;
                    case ColumnType.VARCHAR:
                        bindVariableService.setVarchar(name, readVarchar());
                        break;
                    case ColumnType.BINARY:
                        bindVariableService.setBin(name, readBin(byteViewPool.next()));
                        break;
                    case ColumnType.GEOBYTE:
                        bindVariableService.setGeoHash(name, readByte(), type);
                        break;
                    case ColumnType.GEOSHORT:
                        bindVariableService.setGeoHash(name, readShort(), type);
                        break;
                    case ColumnType.GEOINT:
                        bindVariableService.setGeoHash(name, readInt(), type);
                        break;
                    case ColumnType.GEOLONG:
                        bindVariableService.setGeoHash(name, readLong(), type);
                        break;
                    case ColumnType.UUID:
                        bindVariableService.setUuid(name, readLong(), readLong());
                        break;
                    case ColumnType.ARRAY:
                        // Multiple arrayView objects might be bind to variables, and in `ArrayBindVariable`,
                        // arrayView does not clone its meta information, so `arrayViewPool` is needed.
                        // Same as `setBin`
                        bindVariableService.setArray(i, readArray(arrayViewPool.next()));
                        break;
                    case ColumnType.DECIMAL8:
                        byte decimal8 = readByte();
                        long s = decimal8 < 0 ? -1 : 0;
                        bindVariableService.setDecimal(name, s, s, s, decimal8, type);
                        break;
                    case ColumnType.DECIMAL16:
                        short decimal16 = readShort();
                        s = decimal16 < 0 ? -1 : 0;
                        bindVariableService.setDecimal(name, s, s, s, decimal16, type);
                        break;
                    case ColumnType.DECIMAL32:
                        int decimal32 = readInt();
                        s = decimal32 < 0 ? -1 : 0;
                        bindVariableService.setDecimal(name, s, s, s, decimal32, type);
                        break;
                    case ColumnType.DECIMAL64:
                        long decimal64 = readLong();
                        s = decimal64 < 0 ? -1 : 0;
                        bindVariableService.setDecimal(name, s, s, s, decimal64, type);
                        break;
                    case ColumnType.DECIMAL128:
                        long hi = readLong();
                        long lo = readLong();
                        s = hi < 0 ? -1 : 0;
                        bindVariableService.setDecimal(name, s, s, hi, lo, type);
                        break;
                    case ColumnType.DECIMAL256:
                        bindVariableService.setDecimal(name, readLong(), readLong(), readLong(), readLong(), type);
                        break;
                    default:
                        throw new UnsupportedOperationException("unsupported column type: " + ColumnType.nameOf(type));
                }
            }
        }

        private void read() {
            cmdType = readInt();
            sql.clear();
            sql.put(readStr());
            rndSeed0 = readLong();
            rndSeed1 = readLong();
            arrayViewPool.clear();
            byteViewPool.clear();
        }
    }
}
