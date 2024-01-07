/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.DataFrameCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import org.jetbrains.annotations.NotNull;

public class LatestByAllIndexedRecordCursorFactory extends AbstractTreeSetRecordCursorFactory {
    protected final DirectLongList prefixes;

    public LatestByAllIndexedRecordCursorFactory(
            @NotNull RecordMetadata metadata,
            @NotNull CairoConfiguration configuration,
            @NotNull DataFrameCursorFactory dataFrameCursorFactory,
            int columnIndex,
            @NotNull IntList columnIndexes,
            @NotNull LongList prefixes
    ) {
        super(metadata, dataFrameCursorFactory, configuration);
        this.prefixes = new DirectLongList(Math.max(2, prefixes.size()), MemoryTag.NATIVE_LATEST_BY_LONG_LIST);

        // copy into owned direct memory
        for (int i = 0; i < prefixes.size(); i++) {
            this.prefixes.add(prefixes.get(i));
        }

        this.cursor = new LatestByAllIndexedRecordCursor(columnIndex, rows, columnIndexes, this.prefixes);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("LatestByAllIndexed");
        sink.child(cursor);
        sink.child(dataFrameCursorFactory);
    }

    @Override
    public boolean usesIndex() {
        return true;
    }

    @Override
    protected void _close() {
        super._close();
        prefixes.close();
    }
}
