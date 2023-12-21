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

package io.questdb.cairo.map;

import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.CharSinkBase;

public class ShardedMapCursor implements MapRecordCursor {
    private final ShardedMapRecord recordA = new ShardedMapRecord(true);
    private final ShardedMapRecord recordB = new ShardedMapRecord(false);
    private final ObjList<MapRecordCursor> shardCursors = new ObjList<>();
    private MapRecordCursor currentCursor;
    private int currentIndex;

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        for (int i = currentIndex, n = shardCursors.size(); i < n; i++) {
            shardCursors.getQuick(i).calculateSize(circuitBreaker, counter);
        }
    }

    @Override
    public void close() {
        Misc.freeObjList(shardCursors);
    }

    @Override
    public MapRecord getRecord() {
        return recordA;
    }

    @Override
    public MapRecord getRecordB() {
        return recordB;
    }

    @Override
    public boolean hasNext() throws DataUnavailableException {
        if (currentCursor.hasNext()) {
            recordA.of(currentCursor.getRecord(), currentIndex);
            return true;
        }
        int n = shardCursors.size();
        while (++currentIndex < n) {
            currentCursor = shardCursors.getQuick(currentIndex);
            if (currentCursor.hasNext()) {
                recordA.of(currentCursor.getRecord(), currentIndex);
                return true;
            }
        }
        return false;
    }

    public void of(ObjList<Map> shards) {
        shardCursors.clear();
        for (int i = 0, n = shards.size(); i < n; i++) {
            shardCursors.add(shards.getQuick(i).getCursor());
        }
        toTop();
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        int shardIndex = toShardIndex(atRowId);
        long rowId = toShardRowId(atRowId);
        ((ShardedMapRecord) record).at(shardIndex, rowId);
    }

    @Override
    public long size() throws DataUnavailableException {
        long size = 0;
        for (int i = 0, n = shardCursors.size(); i < n; i++) {
            size += shardCursors.getQuick(i).size();
        }
        return size;
    }

    @Override
    public void toTop() {
        for (int i = 0, n = shardCursors.size(); i < n; i++) {
            shardCursors.getQuick(i).toTop();
        }
        currentCursor = shardCursors.getQuick(0);
        currentIndex = 0;
    }

    private static long toRowId(int shardIndex, long shardRowId) {
        // Most HW uses 48 bits for virtual addresses, so we use the spare 16 bits.
        return (((long) shardIndex) << 48L) + shardRowId;
    }

    private static int toShardIndex(long rowId) {
        return (int) (rowId >>> 48);
    }

    private static long toShardRowId(long rowId) {
        return rowId & 0xFFFFFFFFFFFFL;
    }

    private class ShardedMapRecord implements MapRecord {
        private final boolean isPrimary;
        private MapRecord baseRecord;
        private int shardIndex;

        private ShardedMapRecord(boolean isPrimary) {
            this.isPrimary = isPrimary;
        }

        @Override
        public void copyToKey(MapKey destKey) {
            baseRecord.copyToKey(destKey);
        }

        @Override
        public BinarySequence getBin(int columnIndex) {
            return baseRecord.getBin(columnIndex);
        }

        @Override
        public long getBinLen(int columnIndex) {
            return baseRecord.getBinLen(columnIndex);
        }

        @Override
        public boolean getBool(int columnIndex) {
            return baseRecord.getBool(columnIndex);
        }

        @Override
        public byte getByte(int columnIndex) {
            return baseRecord.getByte(columnIndex);
        }

        @Override
        public char getChar(int columnIndex) {
            return baseRecord.getChar(columnIndex);
        }

        @Override
        public double getDouble(int columnIndex) {
            return baseRecord.getDouble(columnIndex);
        }

        @Override
        public float getFloat(int columnIndex) {
            return baseRecord.getFloat(columnIndex);
        }

        @Override
        public byte getGeoByte(int columnIndex) {
            return baseRecord.getGeoByte(columnIndex);
        }

        @Override
        public int getGeoInt(int columnIndex) {
            return baseRecord.getGeoInt(columnIndex);
        }

        @Override
        public long getGeoLong(int columnIndex) {
            return baseRecord.getGeoLong(columnIndex);
        }

        @Override
        public short getGeoShort(int columnIndex) {
            return baseRecord.getGeoShort(columnIndex);
        }

        @Override
        public int getIPv4(int columnIndex) {
            return baseRecord.getIPv4(columnIndex);
        }

        @Override
        public int getInt(int columnIndex) {
            return baseRecord.getInt(columnIndex);
        }

        @Override
        public long getLong(int columnIndex) {
            return baseRecord.getLong(columnIndex);
        }

        @Override
        public long getLong128Hi(int columnIndex) {
            return baseRecord.getLong128Hi(columnIndex);
        }

        @Override
        public long getLong128Lo(int columnIndex) {
            return baseRecord.getLong128Lo(columnIndex);
        }

        @Override
        public void getLong256(int columnIndex, CharSinkBase<?> sink) {
            baseRecord.getLong256(columnIndex, sink);
        }

        @Override
        public Long256 getLong256A(int columnIndex) {
            return baseRecord.getLong256A(columnIndex);
        }

        @Override
        public Long256 getLong256B(int columnIndex) {
            return baseRecord.getLong256B(columnIndex);
        }

        @Override
        public long getRowId() {
            return toRowId(shardIndex, baseRecord.getRowId());
        }

        @Override
        public short getShort(int columnIndex) {
            return baseRecord.getShort(columnIndex);
        }

        @Override
        public CharSequence getStr(int columnIndex) {
            return baseRecord.getStr(columnIndex);
        }

        @Override
        public void getStr(int columnIndex, CharSink sink) {
            baseRecord.getStr(columnIndex, sink);
        }

        @Override
        public CharSequence getStrB(int columnIndex) {
            return baseRecord.getStrB(columnIndex);
        }

        @Override
        public int getStrLen(int columnIndex) {
            return baseRecord.getStrLen(columnIndex);
        }

        @Override
        public CharSequence getSym(int columnIndex) {
            return baseRecord.getSym(columnIndex);
        }

        @Override
        public CharSequence getSymB(int columnIndex) {
            return baseRecord.getSymB(columnIndex);
        }

        @Override
        public MapValue getValue() {
            return baseRecord.getValue();
        }

        @Override
        public void setSymbolTableResolver(RecordCursor resolver, IntList symbolTableIndex) {
            for (int i = 0, n = shardCursors.size(); i < n; i++) {
                if (isPrimary) {
                    shardCursors.getQuick(i).getRecord().setSymbolTableResolver(resolver, symbolTableIndex);
                } else {
                    shardCursors.getQuick(i).getRecordB().setSymbolTableResolver(resolver, symbolTableIndex);
                }
            }
        }

        void at(int shardIndex, long rowId) {
            MapRecordCursor baseCursor = shardCursors.getQuick(shardIndex);
            if (isPrimary) {
                this.baseRecord = baseCursor.getRecord();
            } else {
                this.baseRecord = baseCursor.getRecordB();
            }
            this.shardIndex = shardIndex;
            baseCursor.recordAt(baseRecord, rowId);
        }

        void of(MapRecord baseRecord, int shardIndex) {
            this.baseRecord = baseRecord;
            this.shardIndex = shardIndex;
        }
    }
}
