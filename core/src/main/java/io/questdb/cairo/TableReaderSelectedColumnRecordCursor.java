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

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Rows;

public class TableReaderSelectedColumnRecordCursor implements RecordCursor {

    protected final TableReaderSelectedColumnRecord recordA;
    private final TableReaderSelectedColumnRecord recordB;
    private final IntList columnIndexes;
    protected TableReader reader;
    private int partitionIndex = 0;
    private int partitionLimit;
    private long maxRecordIndex = -1;
    private int partitionLo;
    private long recordLo;
    private int partitionHi;
    private long recordHi;

    public TableReaderSelectedColumnRecordCursor(IntList columnIndexes) {
        this.columnIndexes = columnIndexes;
        this.recordA = new TableReaderSelectedColumnRecord(columnIndexes);
        this.recordB = new TableReaderSelectedColumnRecord(columnIndexes);
    }

    @Override
    public void close() {
        reader = Misc.free(reader);
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return reader.getSymbolMapReader(columnIndexes.getQuick(columnIndex));
    }

    @Override
    public boolean hasNext() {
        if (recordA.getAdjustedRecordIndex() < maxRecordIndex || switchPartition()) {
            recordA.incrementRecordIndex();
            return true;
        }
        return false;
    }

    @Override
    public Record getRecordB() {
        return recordB;
    }

    @Override
    public void recordAt(Record record, long rowId) {
        ((TableReaderSelectedColumnRecord) record).jumpTo(Rows.toPartitionIndex(rowId), Rows.toLocalRowID(rowId));
    }

    @Override
    public void toTop() {
        partitionIndex = partitionLo;
        if (recordHi == -1) {
            partitionLimit = reader.getPartitionCount();
        } else {
            partitionLimit = Math.min(partitionHi + 1, reader.getPartitionCount());
        }
        maxRecordIndex = recordLo - 1;
        recordA.jumpTo(0, maxRecordIndex);
    }

    @Override
    public long size() {
        return reader.size();
    }

    public void of(TableReader reader) {
        this.partitionLo = 0;
        this.recordLo = 0;
        this.partitionHi = reader.getPartitionCount();
        // because we set partitionHi to partition count
        // the recordHi value becomes irrelevant - partition index never gets to partitionCount.
        this.recordHi = -1;
        of0(reader);
    }

    public void of(TableReader reader, int partitionLo, long recordLo, int partitionHi, long recordHi) {
        this.partitionLo = partitionLo;
        this.partitionHi = partitionHi;
        this.recordLo = recordLo;
        this.recordHi = recordHi;
        of0(reader);
    }

    private void of0(TableReader reader) {
        close();
        this.reader = reader;
        this.recordA.of(reader);
        this.recordB.of(reader);
        toTop();
    }

    private boolean switchPartition() {
        while (partitionIndex < partitionLimit) {
            final long partitionSize = reader.openPartition(partitionIndex);
            if (partitionSize > 0) {
                if (partitionIndex == partitionHi && recordHi > -1) {
                    maxRecordIndex = recordHi - 1;
                } else {
                    maxRecordIndex = partitionSize - 1;
                }
                recordA.jumpTo(partitionIndex, -1);
                partitionIndex++;
                return true;
            }
            partitionIndex++;
        }
        return false;
    }
}
