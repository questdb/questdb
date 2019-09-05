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

package io.questdb.griffin.engine.union;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;

public class UnionRecordCursorFactory implements RecordCursorFactory {
    private final RecordMetadata metadata;
    private final RecordCursorFactory masterFactory;
    private final RecordCursorFactory slaveFactory;
    private final UnionRecordCursor cursor;

    public UnionRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            RecordSink recordSink,
            ColumnTypes valueTypes,
            IntList masterIndexMap,
            IntList slaveIndexMap
    ) {
        this.metadata = metadata;
        this.masterFactory = masterFactory;
        this.slaveFactory = slaveFactory;

        this.cursor = new UnionRecordCursor(
                MapFactory.createMap(configuration, metadata, valueTypes),
                masterIndexMap,
                slaveIndexMap,
                recordSink
        );
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        cursor.of(
                masterFactory.getCursor(executionContext),
                slaveFactory.getCursor(executionContext)
        );
        return cursor;
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean isRandomAccessCursor() {
        return false;
    }
}
