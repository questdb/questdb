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
import io.questdb.std.Rows;

public class TableReaderRecordCursor implements RecordCursor {

    protected final TableReaderRecord record = new TableReaderRecord();
    protected TableReader reader;
    private int partitionIndex = 0;
    private int partitionCount;
    private long maxRecordIndex = -1;

    @Override
    public void close() {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return reader.getSymbolMapReader(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (record.getRecordIndex() < maxRecordIndex || switchPartition()) {
            record.incrementRecordIndex();
            return true;
        }
        return false;
    }

    @Override
    public Record newRecord() {
        TableReaderRecord record = new TableReaderRecord();
        record.of(reader);
        return record;
    }

    @Override
    public long size() {
        return reader.size();
    }

    @Override
    public void recordAt(Record record, long rowId) {
        ((TableReaderRecord) record).jumpTo(Rows.toPartitionIndex(rowId), Rows.toLocalRowID(rowId));
    }

    @Override
    public void recordAt(long rowId) {
        recordAt(record, rowId);
    }

    @Override
    public void toTop() {
        partitionIndex = 0;
        partitionCount = reader.getPartitionCount();
        record.jumpTo(0, -1);
        maxRecordIndex = -1;
    }

    public void of(TableReader reader) {
        close();
        this.reader = reader;
        this.record.of(reader);
        toTop();
    }

    public void startFrom(long rowid) {
        partitionIndex = Rows.toPartitionIndex(rowid);
        long recordIndex = Rows.toLocalRowID(rowid);
        record.jumpTo(this.partitionIndex, recordIndex);
        maxRecordIndex = reader.openPartition(partitionIndex) - 1;
        partitionIndex++;
        this.partitionCount = reader.getPartitionCount();
    }

    private boolean switchPartition() {
        if (partitionIndex < partitionCount) {
            return switchPartition0();
        }
        return false;
    }

    private boolean switchPartition0() {
        while (partitionIndex < partitionCount) {
            final long partitionSize = reader.openPartition(partitionIndex);
            if (partitionSize > 0) {
                maxRecordIndex = partitionSize - 1;
                record.jumpTo(partitionIndex, -1);
                partitionIndex++;
                return true;
            }
            partitionIndex++;
        }
        return false;
    }
}
