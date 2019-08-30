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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;

public class SelectedRecordCursorFactory extends AbstractRecordCursorFactory {

    private final RecordCursorFactory base;
    private final SelectedRecordCursor cursor;

    public SelectedRecordCursorFactory(RecordMetadata metadata, IntList columnCrossIndex, RecordCursorFactory base) {
        super(metadata);
        this.base = base;
        this.cursor = new SelectedRecordCursor(columnCrossIndex);
    }

    @Override
    public void close() {
        base.close();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        this.cursor.of(base.getCursor(executionContext));
        return cursor;
    }

    @Override
    public boolean isRandomAccessCursor() {
        return base.isRandomAccessCursor();
    }
}
