/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo;

import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.SymbolTable;
import com.questdb.std.Rows;

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
