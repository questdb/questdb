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

import com.questdb.cairo.AbstractRecordCursorFactory;
import com.questdb.cairo.CairoException;
import com.questdb.cairo.sql.DataFrameCursor;
import com.questdb.cairo.sql.DataFrameCursorFactory;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.griffin.SqlExecutionContext;

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
