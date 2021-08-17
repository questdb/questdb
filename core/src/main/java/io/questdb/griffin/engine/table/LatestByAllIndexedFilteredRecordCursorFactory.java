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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.DataFrameCursorFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class LatestByAllIndexedFilteredRecordCursorFactory extends AbstractTreeSetRecordCursorFactory {
    protected final DirectLongList prefixes;

    public LatestByAllIndexedFilteredRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull DataFrameCursorFactory dataFrameCursorFactory,
            int columnIndex,
            int hashColumnIndex,
            int hashColumnType,
            @Nullable Function filter,
            @NotNull IntList columnIndexes,
            @NotNull CharSequenceHashSet prefixes
    ) {
        super(metadata, dataFrameCursorFactory, configuration);

        this.prefixes = new DirectLongList(64);
        if (hashColumnIndex > -1 && ColumnType.isGeoHash(hashColumnType)) {
            GeoHashes.fromStringToBits(prefixes, hashColumnType, this.prefixes);
        }

        if (filter == null) {
            this.cursor = new LatestByAllIndexedRecordCursor(columnIndex, hashColumnIndex, hashColumnType, rows, columnIndexes, this.prefixes);
        } else {
            this.cursor = new LatestByAllIndexedFilteredRecordCursor(columnIndex, hashColumnIndex, hashColumnType, rows, filter, columnIndexes, this.prefixes);
        }
    }

    @Override
    public void close() {
        super.close();
        prefixes.close();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }
}
