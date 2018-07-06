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
import com.questdb.cairo.sql.DataFrameCursorFactory;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.griffin.engine.LongTreeSet;

public class LatestByAllIndexedRecordCursorFactory extends AbstractRecordCursorFactory {
    private final DataFrameCursorFactory dataFrameCursorFactory;
    private final LatestByAllIndexedRecordCursor cursor;
    private final LongTreeSet treeSet;

    public LatestByAllIndexedRecordCursorFactory(RecordMetadata metadata, DataFrameCursorFactory dataFrameCursorFactory, int columnIndex) {
        super(metadata);
        //todo: derive page size from key count for symbol and configuration
        this.treeSet = new LongTreeSet(4 * 1024);
        this.cursor = new LatestByAllIndexedRecordCursor(columnIndex, treeSet);
        this.dataFrameCursorFactory = dataFrameCursorFactory;
    }

    @Override
    public void close() {
        treeSet.close();
    }

    @Override
    public RecordCursor getCursor() {
        cursor.of(dataFrameCursorFactory.getCursor());
        return cursor;
    }
}
