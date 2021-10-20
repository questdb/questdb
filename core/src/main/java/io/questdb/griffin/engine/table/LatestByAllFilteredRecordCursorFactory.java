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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.sql.DataFrameCursorFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.IntList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class LatestByAllFilteredRecordCursorFactory extends AbstractTreeSetRecordCursorFactory {
    private final Map map;

    public LatestByAllFilteredRecordCursorFactory(
            @NotNull RecordMetadata metadata,
            @NotNull CairoConfiguration configuration,
            @NotNull DataFrameCursorFactory dataFrameCursorFactory,
            @NotNull RecordSink recordSink,
            @Transient @NotNull ColumnTypes columnTypes,
            @Nullable Function filter,
            @NotNull IntList columnIndexes
    ) {
        super(metadata, dataFrameCursorFactory, configuration);
        this.map = MapFactory.createMap(configuration, columnTypes);
        if (filter == null) {
            this.cursor = new LatestByAllRecordCursor(map, rows, recordSink, columnIndexes);
        } else {
            this.cursor = new LatestByAllFilteredRecordCursor(map, rows, recordSink, filter, columnIndexes);
        }
    }

    @Override
    public void close() {
        super.close();
        map.close();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }
}
