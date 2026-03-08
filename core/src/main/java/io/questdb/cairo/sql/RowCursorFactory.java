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

package io.questdb.cairo.sql;

import io.questdb.griffin.Plannable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.ObjList;

public interface RowCursorFactory extends Plannable {

    static void init(
            ObjList<? extends RowCursorFactory> factories,
            PageFrameCursor pageFrameCursor,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        for (int i = 0, n = factories.size(); i < n; i++) {
            factories.getQuick(i).init(pageFrameCursor, sqlExecutionContext);
        }
    }

    static void prepareCursor(ObjList<? extends RowCursorFactory> factories, PageFrameCursor pageFrameCursor) {
        for (int i = 0, n = factories.size(); i < n; i++) {
            factories.getQuick(i).prepareCursor(pageFrameCursor);
        }
    }

    RowCursor getCursor(PageFrame pageFrame, PageFrameMemory pageFrameMemory);

    default void init(PageFrameCursor pageFrameCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
        // no-op
    }

    boolean isEntity();

    /**
     * Indicates if the factory uses index
     *
     * @return true if the returned RowCursor is using an index, false otherwise
     */
    default boolean isUsingIndex() {
        return false;
    }

    default void prepareCursor(PageFrameCursor pageFrameCursor) {
        // no-op
    }
}
