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
import com.questdb.cairo.sql.CairoEngine;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.RecordMetadata;

public class EmptyTableRecordCursorFactory extends AbstractRecordCursorFactory {
    private final EmptyTableRecordCursor cursor = new EmptyTableRecordCursor();
    private final CairoEngine engine;
    private final String tableName;
    private final long tableVersion;

    public EmptyTableRecordCursorFactory(RecordMetadata metadata, CairoEngine engine, String tableName, long tableVersion) {
        super(metadata);
        this.engine = engine;
        this.tableName = tableName;
        this.tableVersion = tableVersion;
    }

    @Override
    public RecordCursor getCursor() {
        cursor.of(engine.getReader(tableName, tableVersion));
        return cursor;
    }
}
