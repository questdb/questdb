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

import io.questdb.cairo.sql.DataFrameCursor;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.Rows;

abstract class AbstractRecordListCursor extends AbstractDataFrameRecordCursor {

    protected final DirectLongList rows;
    private long index;

    public AbstractRecordListCursor(DirectLongList rows) {
        this.rows = rows;
    }

    @Override
    public long size() {
        return rows.size();
    }

    abstract protected void buildTreeMap(SqlExecutionContext executionContext);

    @Override
    public boolean hasNext() {
        if (index > -1) {
            long row = rows.get(index--);
            record.jumpTo(Rows.toPartitionIndex(row), Rows.toLocalRowID(row));
            return true;
        }
        return false;
    }

    @Override
    public void toTop() {
        index = rows.size() - 1;
    }

    @Override
    void of(DataFrameCursor dataFrameCursor, SqlExecutionContext executionContext) {
        this.dataFrameCursor = dataFrameCursor;
        this.record.of(dataFrameCursor.getTableReader());
        rows.clear();
        buildTreeMap(executionContext);
        index = rows.size() - 1;
    }
}
