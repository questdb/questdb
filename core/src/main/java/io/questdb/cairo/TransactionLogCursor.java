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
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.MemoryCMARWImpl;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;

public class TransactionLogCursor implements RecordCursor {

    private final ObjList<MemoryMR> columns = new ObjList<>();
    private final TransactionLogRecord recordA = new TransactionLogRecord();
    private final TransactionLogRecord recordB = new TransactionLogRecord();
    private final CairoConfiguration configuration;
    private long rowCount;

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

    public void of(long transactionLogTxn, long rowCount, RecordMetadata metadata) {
        recordA.row = -1;
        recordB.row = -1;
        this.rowCount = rowCount;

        final int columnCount = metadata.getColumnCount();
        this.columns.setPos(columnCount * 2);

        final Path path = Path.getThreadLocal(configuration.getRoot());
        path.concat("log").put('.').put(transactionLogTxn);
        int plen = path.length();

        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            MemoryMR col = columns.getQuick(i * 2);
            if (col == null) {
                col = new MemoryCMARWImpl();
                columns.setQuick(i * 2, col);
            }

            if (metadata.getColumnType(i) == ColumnType.STRING || metadata.getColumnType(i) == ColumnType.BINARY) {
                col.wholeFile(
                        configuration.getFilesFacade(),
                        path.trimTo(plen).concat(TableUtils.iFile(path, metadata.getColumnName(i)))
                );

                col = columns.getQuick(i * 2 + 1);
                if (col == null) {
                    col = new MemoryCMARWImpl();
                    columns.setQuick(i * 2 + 1, col);
                }

                col.wholeFile(
                        configuration.getFilesFacade(),
                        path.trimTo(plen).concat(TableUtils.dFile(path, metadata.getColumnName(i)))
                );
            } else {
                col.wholeFile(
                        configuration.getFilesFacade(),
                        path.trimTo(plen).concat(TableUtils.dFile(path, metadata.getColumnName(i)))
                );
            }

        }
    }

    private class TransactionLogRecord implements Record {
        private long row;

        @Override
        public int getInt(int col) {
            return columns.getQuick(col * 2).getInt(row * Integer.BYTES);
        }

        @Override
        public long getRowId() {
            return row;
        }
    }
}
