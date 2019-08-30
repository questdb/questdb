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

package io.questdb.griffin.engine.orderby;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlExecutionContext;

public class SortedRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final RecordTreeChain chain;
    private final SortedRecordCursor cursor;

    public SortedRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory base,
            ColumnTypes columnTypes,
            RecordSink recordSink,
            RecordComparator comparator) {
        super(metadata);
        this.chain = new RecordTreeChain(
                columnTypes,
                recordSink,
                comparator,
                configuration.getSqlSortKeyPageSize(),
                configuration.getSqlSortValuePageSize());
        this.base = base;
        this.cursor = new SortedRecordCursor(chain);
    }

    @Override
    public void close() {
        base.close();
        chain.close();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        this.cursor.of(base.getCursor(executionContext));
        return cursor;
    }

    @Override
    public boolean isRandomAccessCursor() {
        return true;
    }
}
