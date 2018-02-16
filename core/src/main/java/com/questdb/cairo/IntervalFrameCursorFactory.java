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

package com.questdb.cairo;

import com.questdb.cairo.sql.CairoEngine;
import com.questdb.cairo.sql.DataFrameCursor;
import com.questdb.cairo.sql.DataFrameCursorFactory;
import com.questdb.std.LongList;

public class IntervalFrameCursorFactory implements DataFrameCursorFactory {
    private final CairoEngine engine;
    private final String tableName;
    private final IntervalFrameCursor cursor;

    public IntervalFrameCursorFactory(CairoEngine engine, String tableName, LongList intervals) {
        this.engine = engine;
        this.tableName = tableName;
        try (TableReader reader = engine.getReader(tableName)) {
            this.cursor = new IntervalFrameCursor(intervals, reader.getMetadata().getTimestampIndex());
        }
    }

    @Override
    public DataFrameCursor getCursor() {
        cursor.of(engine.getReader(tableName));
        return cursor;
    }
}
