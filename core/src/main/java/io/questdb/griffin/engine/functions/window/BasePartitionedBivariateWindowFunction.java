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

package io.questdb.griffin.engine.functions.window;

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

public abstract class BasePartitionedBivariateWindowFunction extends BaseBivariateWindowFunction implements Reopenable {
    // Non-final so compactPartitionMap can swap the Map instance. Single-writer
    // (refresh worker), no synchronization needed.
    protected Map map;
    protected final VirtualRecord partitionByRecord;
    protected final RecordSink partitionBySink;
    // Live-view tombstone bookkeeping; mirrors BasePartitionedWindowFunction.
    // Subclasses set tombstoneValueIndex in their constructor.
    protected int tombstoneValueIndex = -1;
    protected long tombstoneCount;

    public BasePartitionedBivariateWindowFunction(
            Map map,
            VirtualRecord partitionByRecord,
            RecordSink partitionBySink,
            Function argY,
            Function argX
    ) {
        super(argY, argX);
        this.map = map;
        this.partitionByRecord = partitionByRecord;
        this.partitionBySink = partitionBySink;
    }

    @Override
    public void close() {
        super.close();
        Misc.free(map);
        Misc.freeObjList(partitionByRecord.getFunctions());
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        super.init(symbolTableSource, executionContext);
        Function.init(partitionByRecord.getFunctions(), symbolTableSource, executionContext, null);
    }

    @Override
    public void markPartitionAlive(Record record) {
        if (tombstoneValueIndex < 0 || tombstoneCount == 0) {
            return;
        }
        partitionByRecord.of(record);
        MapKey key = map.withKey();
        key.put(partitionByRecord, partitionBySink);
        MapValue value = key.findValue();
        if (value != null && value.getByte(tombstoneValueIndex) == 1) {
            value.putByte(tombstoneValueIndex, (byte) 0);
            tombstoneCount--;
        }
    }

    @Override
    public void reopen() {
        if (map != null) {
            map.reopen();
        }
        tombstoneCount = 0;
    }

    @Override
    public void reset() {
        Misc.free(map);
        tombstoneCount = 0;
    }

    @Override
    public void toTop() {
        super.toTop();
        Misc.clear(map);
        tombstoneCount = 0;
    }
}
