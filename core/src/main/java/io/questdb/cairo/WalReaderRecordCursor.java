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
import io.questdb.std.Rows;

public class WalReaderRecordCursor implements RecordCursor {
    private final WalReaderRecord recordA = new WalReaderRecord();
    private final WalReaderRecord recordB = new WalReaderRecord();
    private WalReader reader;
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
        return recordA;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return reader.getSymbolMapReader(columnIndex);
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return reader.newSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (recordA.getRecordIndex() < maxRecordIndex) {
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
    public long size() {
        return reader.size();
    }

    @Override
    public void recordAt(Record record, long rowId) {
        ((WalReaderRecord) record).jumpTo(Rows.toLocalRowID(rowId));
    }

    @Override
    public void toTop() {
        recordA.jumpTo(-1);
    }

    public void of(WalReader reader) {
        close();
        this.reader = reader;
        this.recordA.of(reader);
        this.recordB.of(reader);
        openSegment();
    }

    private void openSegment() {
        final long segmentSize = reader.openSegment();
        maxRecordIndex = segmentSize - 1;
        recordA.jumpTo(-1);
    }
}
