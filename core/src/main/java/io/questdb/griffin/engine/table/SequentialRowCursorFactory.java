/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.ObjList;

/**
 * Returns rows from current page frame in order of cursors list:
 * - first fetches and returns all records from first cursor
 * - then from second cursor, third, ...
 * until all cursors are exhausted.
 */
public class SequentialRowCursorFactory implements RowCursorFactory {
    private final SequentialRowCursor cursor;
    private final ObjList<? extends RowCursorFactory> cursorFactories;
    private final int[] cursorFactoriesIdx;
    private final ObjList<RowCursor> cursors;

    public SequentialRowCursorFactory(ObjList<? extends RowCursorFactory> cursorFactories, int[] cursorFactoriesIdx) {
        this.cursorFactories = cursorFactories;
        cursors = new ObjList<>();
        cursor = new SequentialRowCursor();
        this.cursorFactoriesIdx = cursorFactoriesIdx;
    }

    @Override
    public RowCursor getCursor(PageFrame pageFrame, PageFrameMemory pageFrameMemory) {
        for (int i = 0, n = cursorFactoriesIdx[0]; i < n; i++) {
            cursors.extendAndSet(i, cursorFactories.getQuick(i).getCursor(pageFrame, pageFrameMemory));
        }
        cursor.init();
        return cursor;
    }

    @Override
    public void init(PageFrameCursor pageFrameCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
        RowCursorFactory.init(cursorFactories, pageFrameCursor, sqlExecutionContext);
    }

    @Override
    public boolean isEntity() {
        return false;
    }

    public boolean isUsingIndex() {
        return true;
    }

    @Override
    public void prepareCursor(PageFrameCursor pageFrameCursor) {
        RowCursorFactory.prepareCursor(cursorFactories, pageFrameCursor);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Cursor-order scan"); // postgres uses 'Append' node
        for (int i = 0, n = cursorFactories.size(); i < n; i++) {
            sink.child(cursorFactories.getQuick(i));
        }
    }

    private class SequentialRowCursor implements RowCursor {
        private RowCursor currentCursor;
        private int cursorIndex = 0;

        @Override
        public boolean hasNext() {
            boolean hasNext = currentCursor.hasNext();
            if (hasNext) {
                return true;
            }

            while (cursorIndex < cursorFactoriesIdx[0] - 1) {
                currentCursor = cursors.getQuick(++cursorIndex);
                if (currentCursor.hasNext()) {
                    return true;
                }
            }

            return false;
        }

        @Override
        public long next() {
            return currentCursor.next();
        }

        private void init() {
            cursorIndex = 0;
            if (cursorIndex < cursorFactoriesIdx[0]) {
                currentCursor = cursors.getQuick(cursorIndex);
            }
        }
    }
}
