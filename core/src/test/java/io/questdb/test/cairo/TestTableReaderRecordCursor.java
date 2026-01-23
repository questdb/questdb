/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.cairo;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.Rows;

public class TestTableReaderRecordCursor implements RecordCursor {
    protected final TestTableReaderRecord recordA = new TestTableReaderRecord();
    private final TestTableReaderRecord recordB = new TestTableReaderRecord();
    protected TableReader reader;
    private long maxRecordIndex = -1;
    private int partitionHi;
    private int partitionIndex = 0;
    private int partitionLimit;
    private int partitionLo;
    private long recordHi;
    private long recordLo;

    @Override
    public void close() {
        // no-op
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public Record getRecordB() {
        return recordB;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return reader.getSymbolMapReader(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (recordA.getRecordIndex() < maxRecordIndex || switchPartition()) {
            recordA.incrementRecordIndex();
            return true;
        }
        return false;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return reader.newSymbolTable(columnIndex);
    }

    public TestTableReaderRecordCursor of(TableReader reader) {
        this.partitionLo = 0;
        this.recordLo = 0;
        this.partitionHi = reader.getPartitionCount();
        // because we set partitionHi to partition count
        // the recordHi value becomes irrelevant - partition index never gets to partitionCount.
        this.recordHi = -1;
        of0(reader);
        return this;
    }

    public void of(TableReader reader, int partitionLo, long recordLo, int partitionHi, long recordHi) {
        this.partitionLo = partitionLo;
        this.partitionHi = partitionHi;
        this.recordLo = recordLo;
        this.recordHi = recordHi;
        of0(reader);
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public void recordAt(Record record, long rowId) {
        ((TestTableReaderRecord) record).jumpTo(Rows.toPartitionIndex(rowId), Rows.toLocalRowID(rowId));
    }

    @Override
    public long size() {
        return reader.size();
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

    private void of0(TableReader reader) {
        close();
        this.reader = reader;
        this.recordA.of(reader);
        this.recordB.of(reader);
        toTop();
    }

    private boolean switchPartition() {
        if (partitionIndex < partitionLimit) {
            return switchPartition0();
        }
        return false;
    }

    private boolean switchPartition0() {
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

    protected void startFrom(long rowid) {
        partitionIndex = Rows.toPartitionIndex(rowid);
        long recordIndex = Rows.toLocalRowID(rowid);
        recordA.jumpTo(this.partitionIndex, recordIndex);
        maxRecordIndex = reader.openPartition(partitionIndex) - 1;
        partitionIndex++;
        this.partitionLimit = reader.getPartitionCount();
    }
}
