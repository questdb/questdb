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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.DataFrameCursor;
import io.questdb.cairo.sql.DataFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlExecutionContext;

abstract class AbstractDataFrameRecordCursorFactory extends AbstractRecordCursorFactory {
    protected final DataFrameCursorFactory dataFrameCursorFactory;

    public AbstractDataFrameRecordCursorFactory(RecordMetadata metadata, DataFrameCursorFactory dataFrameCursorFactory) {
        super(metadata);
        this.dataFrameCursorFactory = dataFrameCursorFactory;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        DataFrameCursor dataFrameCursor = dataFrameCursorFactory.getCursor(executionContext.getCairoSecurityContext());
        try {
            return getCursorInstance(dataFrameCursor, executionContext);
        } catch (CairoException e) {
            dataFrameCursor.close();
            throw e;
        }
    }

    protected abstract RecordCursor getCursorInstance(
            DataFrameCursor dataFrameCursor,
            SqlExecutionContext executionContext
    );
}
