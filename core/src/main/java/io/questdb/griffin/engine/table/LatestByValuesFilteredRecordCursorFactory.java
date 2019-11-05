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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.sql.DataFrameCursor;
import io.questdb.cairo.sql.DataFrameCursorFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class LatestByValuesFilteredRecordCursorFactory extends AbstractDeferredTreeSetRecordCursorFactory {

    private final Function filter;

    public LatestByValuesFilteredRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull DataFrameCursorFactory dataFrameCursorFactory,
            int columnIndex,
            @Transient CharSequenceHashSet keyValues,
            @Transient SymbolMapReader symbolMapReader,
            @Nullable Function filter
    ) {
        super(configuration, metadata, dataFrameCursorFactory, columnIndex, keyValues, symbolMapReader);
        if (filter != null) {
            this.cursor = new LatestByValuesFilteredRecordCursor(columnIndex, rows, symbolKeys, filter);
        } else {
            this.cursor = new LatestByValuesRecordCursor(columnIndex, rows, symbolKeys);
        }
        this.filter = filter;
    }

    @Override
    public void close() {
        super.close();
        if (filter != null) {
            filter.close();
        }
    }

    @Override
    public boolean isRandomAccessCursor() {
        return true;
    }

    @Override
    protected AbstractDataFrameRecordCursor getCursorInstance(
            DataFrameCursor dataFrameCursor,
            SqlExecutionContext executionContext
    ) {
        if (filter != null) {
            AbstractDataFrameRecordCursor cursor = super.getCursorInstance(dataFrameCursor, executionContext);
            filter.init(cursor, executionContext);
            return cursor;
        }
        return super.getCursorInstance(dataFrameCursor, executionContext);
    }
}
