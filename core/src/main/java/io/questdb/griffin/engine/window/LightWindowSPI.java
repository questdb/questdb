/*+*****************************************************************************
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

package io.questdb.griffin.engine.window;

import io.questdb.cairo.RecordArray;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;

final class LightWindowSPI implements WindowSPI {
    private final DirectLongList baseRowIds;
    private final WindowLightRecord lookupRecord;
    private final RecordArray narrowChain;
    private final IntList sourceMap;
    private RecordCursor baseCursor;
    private Record lookupRecordBase;
    private Record lookupRecordNarrow;

    LightWindowSPI(IntList sourceMap, RecordArray narrowChain, DirectLongList baseRowIds) {
        this.sourceMap = sourceMap;
        this.narrowChain = narrowChain;
        this.baseRowIds = baseRowIds;
        this.lookupRecord = new WindowLightRecord(sourceMap);
    }

    @Override
    public long getAddress(long rowIndex, int chainColIdx) {
        int encoded = sourceMap.getQuick(chainColIdx);
        assert encoded < 0;
        return narrowChain.getAddressAtRowIndex(rowIndex, -encoded - 1);
    }

    @Override
    public Record getRecordAt(long rowIndex) {
        baseCursor.recordAt(lookupRecordBase, baseRowIds.get(rowIndex));
        narrowChain.recordAtRowIndex(lookupRecordNarrow, rowIndex);
        lookupRecord.of(lookupRecordBase, lookupRecordNarrow, rowIndex);
        return lookupRecord;
    }

    void of(RecordCursor baseCursor) {
        this.baseCursor = baseCursor;
        this.lookupRecordBase = baseCursor.getRecordB();
        this.lookupRecordNarrow = narrowChain.getRecordB();
    }
}
