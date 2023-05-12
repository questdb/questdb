/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.table;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.DataFrame;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.RowCursorFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.ObjList;

/**
 * Returns rows from current data frame in table (physical) order :
 * - fetches first record index/row id per cursor into priority queue
 * - then returns record with the smallest available index and adds next record from related cursor into queue
 * until all cursors are exhausted .
 */
public class HeapRowCursorFactory implements RowCursorFactory {
    private final HeapRowCursor cursor;
    private final ObjList<? extends RowCursorFactory> cursorFactories;
    // used to skip some cursor factories if values repeat
    private final int[] cursorFactoriesIdx;
    private final ObjList<RowCursor> cursors;

    public HeapRowCursorFactory(ObjList<? extends RowCursorFactory> cursorFactories, int[] cursorFactoriesIdx) {
        this.cursorFactories = cursorFactories;
        this.cursors = new ObjList<>();
        this.cursor = new HeapRowCursor();
        this.cursorFactoriesIdx = cursorFactoriesIdx;
    }

    @Override
    public RowCursor getCursor(DataFrame dataFrame) {
        for (int i = 0, n = cursorFactories.size(); i < n; i++) {
            cursors.extendAndSet(i, cursorFactories.getQuick(i).getCursor(dataFrame));
        }
        cursor.of(cursors, cursorFactoriesIdx[0]);
        return cursor;
    }

    @Override
    public void init(TableReader tableReader, SqlExecutionContext sqlExecutionContext) throws SqlException {
        RowCursorFactory.init(cursorFactories, tableReader, sqlExecutionContext);
    }

    @Override
    public boolean isEntity() {
        return false;
    }

    @Override
    public void prepareCursor(TableReader tableReader) {
        RowCursorFactory.prepareCursor(cursorFactories, tableReader);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Table-order scan");
        for (int i = 0, n = cursorFactories.size(); i < n; i++) {
            sink.child(cursorFactories.getQuick(i));
        }
    }
}
