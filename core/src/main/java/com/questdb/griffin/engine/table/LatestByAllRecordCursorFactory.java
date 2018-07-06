/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

import com.questdb.cairo.AbstractRecordCursorFactory;
import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.ColumnTypes;
import com.questdb.cairo.map.FastMap;
import com.questdb.cairo.map.Map;
import com.questdb.cairo.map.RecordSink;
import com.questdb.cairo.sql.DataFrameCursorFactory;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.griffin.engine.LongTreeSet;
import com.questdb.std.Transient;

public class LatestByAllRecordCursorFactory extends AbstractRecordCursorFactory {
    private final DataFrameCursorFactory dataFrameCursorFactory;
    private final LatestByAllRecordCursor cursor;
    private final Map map;
    private final LongTreeSet treeSet;

    public LatestByAllRecordCursorFactory(
            RecordMetadata metadata,
            CairoConfiguration configuration,
            DataFrameCursorFactory dataFrameCursorFactory,
            RecordSink recordSink,
            @Transient ColumnTypes columnTypes) {
        super(metadata);
        this.treeSet = new LongTreeSet(configuration.getSqlTreeDefaultPageSize());
        this.map = new FastMap(
                configuration.getSqlMapDefaultPageSize(),
                columnTypes,
                configuration.getSqlMapDefaultKeyCapacity(),
                configuration.getSqlFastMapLoadFactor()
        );
        this.cursor = new LatestByAllRecordCursor(map, treeSet, recordSink);
        this.dataFrameCursorFactory = dataFrameCursorFactory;
    }

    @Override
    public void close() {
        cursor.close();
        treeSet.close();
        map.close();
    }

    @Override
    public RecordCursor getCursor() {
        cursor.of(dataFrameCursorFactory.getCursor());
        return cursor;
    }
}
