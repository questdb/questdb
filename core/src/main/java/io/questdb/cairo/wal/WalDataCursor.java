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

package io.questdb.cairo.wal;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.std.Misc;

public class WalDataCursor implements RecordCursor {
    private final WalDataRecord recordA = new WalDataRecord();
    private long maxRecordIndex = -1;
    private WalReader reader;

    @Override
    public void close() {
        reader = Misc.free(reader);
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public Record getRecordB() {
        throw new UnsupportedOperationException("No B record for WAL");
    }

    @Override
    public boolean hasNext() {
        if (recordA.getRecordIndex() < maxRecordIndex) {
            recordA.incrementRecordIndex();
            return true;
        }
        return false;
    }

    public void of(WalReader reader) {
        close();

        this.reader = reader;
        recordA.of(reader);

        final long segmentSize = reader.openSegment();
        maxRecordIndex = segmentSize - 1;
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public void recordAt(Record record, long rowId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long size() {
        return reader.size();
    }

    @Override
    public void toTop() {
        recordA.jumpTo(-1);
    }
}
