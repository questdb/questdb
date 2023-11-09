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
 *2
 ******************************************************************************/

package io.questdb.griffin.engine.table;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.AnyRecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;

import java.io.Closeable;
import java.io.IOException;

/**
 * Returns rows from current record in order of cursors list :
 * - first fetches and returns all records from first cursor
 * - then from second cursor, third, ...
 * until all cursors are exhausted .
 */
public abstract class SequentialRecordCursorFactory<T extends RecordCursorFactory> implements RecordCursorFactory {
    private final SequentialRecordCursor cursor;
    private final ObjList<T> cursorFactories;
    private final ObjList<RecordCursor> cursors;

    public SequentialRecordCursorFactory() {
        cursorFactories = new ObjList<>();
        cursors = new ObjList<>();
        cursor = new SequentialRecordCursor();
    }

    public abstract RecordMetadata getMetadata();

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }


    public abstract void initFactories(SqlExecutionContext executionContext, ObjList<T> factoriesBucket);

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        for (int i = 0; i < cursorFactories.size(); i++) {
            T factory = cursorFactories.getQuick(i);
            factory.close();
        }
        this.cursorFactories.clear();

        initFactories(executionContext, this.cursorFactories);

        for (int i = 0; i < cursors.size(); i++) {
            RecordCursor cur = cursors.getQuick(i);
            cur.close();
        }
        cursors.clear();

        for (int i = 0; i < cursorFactories.size(); i++) {
            RecordCursor cursor = cursorFactories.getQuick(i).getCursor(executionContext);
            cursors.extendAndSet(i, cursor);
        }
        cursor.init();
        return cursor;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Cursor-order scan");//postgres uses 'Append' node  
        for (int i = 0, n = cursorFactories.size(); i < n; i++) {
            sink.child(cursorFactories.getQuick(i));
        }
    }

    @Override
    public void close() {
        RecordCursorFactory.super.close();
        for (int i = 0; i < cursorFactories.size(); i++) {
            RecordCursorFactory factory = cursorFactories.get(i);
            factory.close();
        }
        cursorFactories.clear();
    }

    private class SequentialRecordCursor implements NoRandomAccessRecordCursor {
        private final SequentialRecord sequentialRecord = new SequentialRecord();

        private RecordCursor currentCursor;
        private int cursorIndex = 0;

        private void init() {
            cursorIndex = 0;
            currentCursor = cursors.getQuiet(0);
            if (currentCursor != null) {
                sequentialRecord.setCurrentRecord(currentCursor.getRecord());
            }
        }

        @Override
        public void close() {
            for (int i = 0; i < cursors.size(); i++) {
                cursors.getQuick(i).close();
            }
            cursors.clear();
        }

        @Override
        public Record getRecord() {
            return sequentialRecord;
        }

        @Override
        public boolean hasNext() {
            if (currentCursor == null) {
                return false;
            }

            boolean hasNext = currentCursor.hasNext();
            if (hasNext) {
                return true;
            }

            while (cursorIndex + 1 < cursors.size()) {
                cursorIndex++;
                currentCursor = cursors.getQuick(cursorIndex);
                sequentialRecord.setCurrentRecord(currentCursor.getRecord());
                hasNext = currentCursor.hasNext();
                if (hasNext) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public long size() throws DataUnavailableException {
            return -1;
        }


        @Override
        public void toTop() {
            for (int i = 0; i < cursors.size(); i++) {
                cursors.getQuick(i).toTop();
            }
            init();
        }


    }

    private static class SequentialRecord implements Record {

        private Record currentRecord;

        public void setCurrentRecord(Record currentRecord) {
            this.currentRecord = currentRecord;
        }

        @Override
        public BinarySequence getBin(int col) {
            return currentRecord.getBin(col);
        }

        @Override
        public long getBinLen(int col) {
            return currentRecord.getBinLen(col);
        }

        @Override
        public boolean getBool(int col) {
            return currentRecord.getBool(col);
        }

        @Override
        public byte getByte(int col) {
            return currentRecord.getByte(col);
        }

        @Override
        public char getChar(int col) {
            return currentRecord.getChar(col);
        }

        @Override
        public long getDate(int col) {
            return currentRecord.getDate(col);
        }

        @Override
        public double getDouble(int col) {
            return currentRecord.getDouble(col);
        }

        @Override
        public float getFloat(int col) {
            return currentRecord.getFloat(col);
        }

        @Override
        public byte getGeoByte(int col) {
            return currentRecord.getGeoByte(col);
        }

        @Override
        public int getGeoInt(int col) {
            return currentRecord.getGeoInt(col);
        }

        @Override
        public long getGeoLong(int col) {
            return currentRecord.getGeoLong(col);
        }

        @Override
        public short getGeoShort(int col) {
            return currentRecord.getGeoShort(col);
        }

        @Override
        public int getIPv4(int col) {
            return currentRecord.getIPv4(col);
        }

        @Override
        public int getInt(int col) {
            return currentRecord.getInt(col);
        }

        @Override
        public long getLong(int col) {
            return currentRecord.getLong(col);
        }

        @Override
        public long getLong128Hi(int col) {
            return currentRecord.getLong128Hi(col);
        }

        @Override
        public long getLong128Lo(int col) {
            return currentRecord.getLong128Lo(col);
        }

        @Override
        public Long256 getLong256A(int col) {
            return currentRecord.getLong256A(col);
        }

        @Override
        public Long256 getLong256B(int col) {
            return currentRecord.getLong256B(col);
        }

        @Override
        public long getLongIPv4(int col) {
            return currentRecord.getLongIPv4(col);
        }

        @Override
        public Record getRecord(int col) {
            return currentRecord.getRecord(col);
        }

        @Override
        public long getRowId() {
            return currentRecord.getRowId();
        }

        @Override
        public short getShort(int col) {
            return currentRecord.getShort(col);
        }

        @Override
        public CharSequence getStr(int col) {
            return currentRecord.getStr(col);
        }

        @Override
        public void getStr(int col, CharSink sink) {
            currentRecord.getStr(col, sink);
        }

        @Override
        public CharSequence getStrB(int col) {
            return currentRecord.getStrB(col);
        }

        @Override
        public int getStrLen(int col) {
            return currentRecord.getStrLen(col);
        }

        @Override
        public CharSequence getSym(int col) {
            return currentRecord.getSym(col);
        }

        @Override
        public CharSequence getSymB(int col) {
            return currentRecord.getSymB(col);
        }

        @Override
        public long getTimestamp(int col) {
            return currentRecord.getTimestamp(col);
        }

        @Override
        public long getUpdateRowId() {
            return currentRecord.getUpdateRowId();
        }
    }
}
