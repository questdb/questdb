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

package io.questdb.cairo;

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.ObjList;

public class WalEventCursor {
    public static final long END_OF_EVENTS = -1L;

    private final DataInfo dataInfo = new DataInfo();
    private final AddColumnInfo addColumnInfo = new AddColumnInfo();
    private final RemoveColumnInfo removeColumnInfo = new RemoveColumnInfo();

    private final MemoryMR eventMem;
    private long memSize;
    private long offset = Integer.BYTES; // skip wal meta version
    private long txn = END_OF_EVENTS;
    private byte type = WalTxnType.NONE;

    public WalEventCursor(MemoryMR eventMem) {
        this.eventMem = eventMem;
    }

    public boolean hasNext() {
        txn = readLong();
        if (txn == END_OF_EVENTS) {
            return false;
        }

        type = readByte();
        switch (type) {
            case WalTxnType.DATA:
                dataInfo.read();
                break;
            case WalTxnType.ADD_COLUMN:
                addColumnInfo.read();
                break;
            case WalTxnType.REMOVE_COLUMN:
                removeColumnInfo.read();
                break;
            default:
                throw CairoException.instance(CairoException.METADATA_VALIDATION).put("Unsupported WAL event type: ").put(type);
        }
        return true;
    }

    public void reset() {
        memSize = eventMem.size();
        offset = Integer.BYTES; // skip wal meta version
        txn = END_OF_EVENTS;
        type = WalTxnType.NONE;
    }

    public DataInfo getDataInfo() {
        return dataInfo;
    }

    public AddColumnInfo getAddColumnInfo() {
        return addColumnInfo;
    }

    public RemoveColumnInfo getRemoveColumnInfo() {
        return removeColumnInfo;
    }

    public long getTxn() {
        return txn;
    }

    public byte getType() {
        return type;
    }

    public class DataInfo {
        private final ObjList<SymbolMapDiff> symbolMapDiffs = new ObjList<>();

        private long startRowID;
        private long endRowID;
        private long minTimestamp;
        private long maxTimestamp;
        private boolean outOfOrder;

        private void read() {
            startRowID = readLong();
            endRowID = readLong();
            minTimestamp = readLong();
            maxTimestamp = readLong();
            outOfOrder = readBool();

            readSymbolMapDiffs(symbolMapDiffs);
        }

        public long getStartRowID() {
            return startRowID;
        }

        public long getEndRowID() {
            return endRowID;
        }

        public long getMinTimestamp() {
            return minTimestamp;
        }

        public long getMaxTimestamp() {
            return maxTimestamp;
        }

        public boolean isOutOfOrder() {
            return outOfOrder;
        }

        public SymbolMapDiff getSymbolMapDiff(int columnIndex) {
            return symbolMapDiffs.getQuiet(columnIndex);
        }
    }

    public class AddColumnInfo {
        private int columnIndex;
        private CharSequence columnName;
        private int columnType;

        private void read() {
            columnIndex = readInt();
            columnName = readStr();
            columnType = readInt();
        }

        public int getColumnIndex() {
            return columnIndex;
        }

        public CharSequence getColumnName() {
            return columnName;
        }

        public int getColumnType() {
            return columnType;
        }
    }

    public class RemoveColumnInfo {
        private int columnIndex;

        private void read() {
            columnIndex = readInt();
        }

        public int getColumnIndex() {
            return columnIndex;
        }
    }

    private long readLong() {
        checkMemSize(Long.BYTES);
        final long value = eventMem.getLong(offset);
        offset += Long.BYTES;
        return value;
    }

    private int readInt() {
        checkMemSize(Integer.BYTES);
        final int value = eventMem.getInt(offset);
        offset += Integer.BYTES;
        return value;
    }

    private byte readByte() {
        checkMemSize(Byte.BYTES);
        final byte value = eventMem.getByte(offset);
        offset += Byte.BYTES;
        return value;
    }

    private boolean readBool() {
        checkMemSize(Byte.BYTES);
        final boolean value = eventMem.getBool(offset);
        offset += Byte.BYTES;
        return value;
    }

    private CharSequence readStr() {
        checkMemSize(Integer.BYTES);
        final int strLength = eventMem.getInt(offset);
        final long storageLength = Vm.getStorageLength(strLength);

        checkMemSize(storageLength);
        final CharSequence value = eventMem.getStr(offset);
        offset += storageLength;
        return value;
    }

    private void readSymbolMapDiffs(ObjList<SymbolMapDiff> symbolMapDiffs) {
        symbolMapDiffs.clear();
        while (true) {
            final int columnIndex = readInt();
            if (columnIndex == SymbolMapDiff.END_OF_SYMBOL_DIFFS) {
                break;
            }

            // do something about the garbage, do not create always new object
            // change API to cursor style maybe... no symbolMapDiffs list and only a single SymbolMapDiff object
            final SymbolMapDiff symbolMapDiff = new SymbolMapDiff();
            symbolMapDiffs.extendAndSet(columnIndex, symbolMapDiff);

            final int numOfNewSymbols = readInt();
            for (int i = 0; i < numOfNewSymbols; i++) {
                final int key = readInt();
                final String symbol = readStr().toString();
                symbolMapDiff.add(symbol, key);
            }
        }
    }

    private void checkMemSize(long requiredBytes) {
        if (memSize < offset + requiredBytes) {
            throw CairoException.instance(0).put("WAL event file is too small, size=").put(memSize).put(", required=").put(offset + requiredBytes);
        }
    }
}
