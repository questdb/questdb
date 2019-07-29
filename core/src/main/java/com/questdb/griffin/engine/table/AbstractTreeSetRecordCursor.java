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

import com.questdb.cairo.sql.DataFrameCursor;
import com.questdb.griffin.SqlExecutionContext;
import com.questdb.std.Rows;

abstract class AbstractTreeSetRecordCursor extends AbstractDataFrameRecordCursor {

    protected final LongTreeSet treeSet;
    private LongTreeSet.TreeCursor treeCursor;

    public AbstractTreeSetRecordCursor(LongTreeSet treeSet) {
        this.treeSet = treeSet;
    }

    @Override
    public void close() {
        super.close();
        treeCursor = null;
    }

    abstract protected void buildTreeMap(SqlExecutionContext executionContext);

    @Override
    public final boolean hasNext() {
        if (treeCursor.hasNext()) {
            long row = treeCursor.next();
            record.jumpTo(Rows.toPartitionIndex(row), Rows.toLocalRowID(row));
            return true;
        }
        return false;
    }

    @Override
    public void toTop() {
        treeCursor.toTop();
    }

    @Override
    final void of(DataFrameCursor dataFrameCursor, SqlExecutionContext executionContext) {
        this.dataFrameCursor = dataFrameCursor;
        this.record.of(dataFrameCursor.getTableReader());
        treeSet.clear();
        buildTreeMap(executionContext);
        treeCursor = treeSet.getCursor();
    }
}
