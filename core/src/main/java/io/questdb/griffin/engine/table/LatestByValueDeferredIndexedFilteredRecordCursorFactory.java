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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.DataFrameCursorFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.IntList;
import org.jetbrains.annotations.NotNull;

public class LatestByValueDeferredIndexedFilteredRecordCursorFactory extends AbstractDeferredValueRecordCursorFactory {

    private final IntList columnIndexes;

    public LatestByValueDeferredIndexedFilteredRecordCursorFactory(
            @NotNull RecordMetadata metadata,
            @NotNull DataFrameCursorFactory dataFrameCursorFactory,
            int columnIndex,
            Function symbolFunc,
            @NotNull Function filter,
            @NotNull IntList columnIndexes
    ) {
        super(metadata, dataFrameCursorFactory, columnIndex, symbolFunc, filter);
        this.columnIndexes = columnIndexes;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    protected AbstractDataFrameRecordCursor createDataFrameCursorFor(int symbolKey) {
        assert filter != null;
        return new LatestByValueIndexedFilteredRecordCursor(columnIndex, TableUtils.toIndexKey(symbolKey), filter, columnIndexes);
    }
}
