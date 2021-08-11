/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.MemoryCMRImpl;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Path;

public class TransactionLogCursor implements RecordCursor {

    private final ObjList<MemoryMR> columns = new ObjList<>();
    private final TransactionLogRecord recordA = new TransactionLogRecord();
    private final TransactionLogRecord recordB = new TransactionLogRecord();
    private final CairoConfiguration configuration;
    private long rowCount;
    private TableReader reader;

    public TransactionLogCursor(CairoConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void close() {
        Misc.freeObjList(columns);
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return reader.getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        return ++recordA.row < rowCount;
    }

    @Override
    public Record getRecordB() {
        return recordB;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        ((TransactionLogRecord) record).row = atRowId;
    }

    @Override
    public void toTop() {
        recordA.row = -1;
    }

    @Override
    public long size() {
        return rowCount;
    }

    public void of(TableReader reader) {
        recordA.row = -1;
        recordB.row = -1;
        this.rowCount = reader.getTransactionLogRowCount();
        this.reader = reader;

        final int columnCount = reader.getColumnCount();
        this.columns.setPos(columnCount * 2);

        final Path path = Path.getThreadLocal(configuration.getRoot());
        path.concat(reader.getTableName()).concat("log").put('.').put(reader.getTransactionLogTxn());
        int plen = path.length();

        for (int i = 0; i < columnCount; i++) {
            MemoryMR col = columns.getQuick(i * 2);
            if (col == null) {
                col = new MemoryCMRImpl();
                columns.setQuick(i * 2, col);
            }

            final int columnType = reader.getMetadata().getColumnType(i);
            final CharSequence columnName = reader.getMetadata().getColumnName(i);

            if (ColumnType.isVariableLength(columnType)) {
                col.wholeFile(
                        configuration.getFilesFacade(),
                        TableUtils.iFile(path.trimTo(plen), columnName)
                );

                col = columns.getQuick(i * 2 + 1);
                if (col == null) {
                    col = new MemoryCMRImpl();
                    columns.setQuick(i * 2 + 1, col);
                }

                col.wholeFile(
                        configuration.getFilesFacade(),
                        TableUtils.dFile(path.trimTo(plen), columnName)
                );
            } else {
                col.wholeFile(
                        configuration.getFilesFacade(),
                        TableUtils.dFile(path.trimTo(plen), columnName)
                );
            }
        }
    }

    private class TransactionLogRecord implements Record {
        private long row;

        @Override
        public BinarySequence getBin(int col) {
            return columns.getQuick(col * 2 + 1).getBin(
                    columns.getQuick(col * 2).getLong(row * Long.BYTES)
            );
        }

        @Override
        public long getBinLen(int col) {
            return columns.getQuick(col * 2 + 1).getBinLen(
                    columns.getQuick(col * 2).getLong(row * Long.BYTES)
            );
        }

        @Override
        public boolean getBool(int col) {
            return columns.getQuick(col * 2).getBool(row);
        }

        @Override
        public byte getByte(int col) {
            return columns.getQuick(col * 2).getByte(row);
        }

        @Override
        public char getChar(int col) {
            return columns.getQuick(col * 2).getChar(row * Character.BYTES);
        }

        @Override
        public long getDate(int col) {
            return columns.getQuick(col * 2).getLong(row * Long.BYTES);
        }

        @Override
        public double getDouble(int col) {
            return columns.getQuick(col * 2).getDouble(row * Double.BYTES);
        }

        @Override
        public float getFloat(int col) {
            return columns.getQuick(col * 2).getFloat(row * Float.BYTES);
        }

        @Override
        public int getInt(int col) {
            return columns.getQuick(col * 2).getInt(row * Integer.BYTES);
        }

        @Override
        public long getLong(int col) {
            return columns.getQuick(col * 2).getLong(row * Long.BYTES);
        }

        @Override
        public void getLong256(int col, CharSink sink) {
            columns.getQuick(col * 2).getLong256(row * Long256.BYTES, sink);
        }

        @Override
        public Long256 getLong256A(int col) {
            return columns.getQuick(col * 2).getLong256A(row * Long256.BYTES);
        }

        @Override
        public Long256 getLong256B(int col) {
            return columns.getQuick(col * 2).getLong256B(row * Long256.BYTES);
        }

        @Override
        public long getRowId() {
            return row;
        }

        @Override
        public short getShort(int col) {
            return columns.getQuick(col * 2).getShort(row * Short.BYTES);
        }

        @Override
        public CharSequence getStr(int col) {
            return columns.getQuick(col * 2 + 1).getStr(
                    columns.getQuick(col * 2).getLong(row * Long.BYTES)
            );
        }

        @Override
        public void getStr(int col, CharSink sink) {
            Record.super.getStr(col, sink);
        }

        @Override
        public CharSequence getStrB(int col) {
            return columns.getQuick(col * 2 + 1).getStr2(
                    columns.getQuick(col * 2).getLong(row * Long.BYTES)
            );
        }

        @Override
        public int getStrLen(int col) {
            return columns.getQuick(col * 2 + 1).getStrLen(
                    columns.getQuick(col * 2).getLong(row * Long.BYTES)
            );
        }

        @Override
        public CharSequence getSym(int col) {
            return getSymbolTable(col).valueOf(
                    columns.getQuick(col * 2).getInt(row * Integer.BYTES)
            );
        }

        @Override
        public CharSequence getSymB(int col) {
            return getSymbolTable(col).valueBOf(
                    columns.getQuick(col * 2).getInt(row * Integer.BYTES)
            );
        }

        @Override
        public long getTimestamp(int col) {
            return columns.getQuick(col * 2).getLong(row * Long.BYTES);
        }
    }
}
