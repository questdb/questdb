/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.table;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.ColumnTypes;
import com.questdb.cairo.RecordSink;
import com.questdb.cairo.map.Map;
import com.questdb.cairo.map.MapFactory;
import com.questdb.cairo.sql.DataFrameCursorFactory;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.std.Transient;
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
            @Nullable Function filter) {
        super(metadata, dataFrameCursorFactory, configuration);
        this.map = MapFactory.createMap(configuration, columnTypes);
        if (filter == null) {
            this.cursor = new LatestByAllRecordCursor(map, treeSet, recordSink);
        } else {
            this.cursor = new LatestByAllFilteredRecordCursor(map, treeSet, recordSink, filter);
        }
    }

    @Override
    public void close() {
        super.close();
        map.close();
    }

    @Override
    public boolean isRandomAccessCursor() {
        return true;
    }
}
