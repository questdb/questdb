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

import com.questdb.cairo.sql.*;
import com.questdb.common.RowCursor;
import org.jetbrains.annotations.Nullable;

class DataFrameRecordCursor extends AbstractDataFrameRecordCursor {
    private final RowCursorFactory rowCursorFactory;
    private RowCursor rowCursor;
    private final Function filter;

    public DataFrameRecordCursor(RowCursorFactory rowCursorFactory, @Nullable Function filter) {
        this.rowCursorFactory = rowCursorFactory;
        this.filter = filter;
    }

    @Override
    public boolean hasNext() {
        try {
            if (rowCursor != null && rowCursor.hasNext()) {
                record.setRecordIndex(rowCursor.next());
                return true;
            }
            return nextFrame();
        } catch (NoMoreFramesException ignore) {
            return false;
        }
    }

    @Override
    public Record next() {
        return record;
    }

    public void of(DataFrameCursor dataFrameCursor) {
        if (this.dataFrameCursor != dataFrameCursor) {
            close();
            this.dataFrameCursor = dataFrameCursor;
        }
        this.record.of(dataFrameCursor.getTableReader());
        this.rowCursorFactory.prepareCursor(dataFrameCursor.getTableReader());
        rowCursor = null;
        if (filter != null) {
            filter.withCursor(this);
        }
    }

    @Override
    public void toTop() {
        dataFrameCursor.toTop();
        rowCursor = null;
        if (filter != null) {
            filter.toTop();
        }
    }

    private boolean nextFrame() {
        while (dataFrameCursor.hasNext()) {
            DataFrame dataFrame = dataFrameCursor.next();
            rowCursor = rowCursorFactory.getCursor(dataFrame);
            if (rowCursor.hasNext()) {
                record.jumpTo(dataFrame.getPartitionIndex(), rowCursor.next());
                return true;
            }
        }
        return false;
    }
}
