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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.Misc;

public abstract class AbstractJoinCursor implements NoRandomAccessRecordCursor {
    protected final int columnSplit;
    protected RecordCursor masterCursor;
    protected RecordCursor slaveCursor;

    public AbstractJoinCursor(int columnSplit) {
        this.columnSplit = columnSplit;
    }

    @Override
    public void close() {
        masterCursor = Misc.free(masterCursor);
        slaveCursor = Misc.free(slaveCursor);
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        if (columnIndex < columnSplit) {
            return masterCursor.getSymbolTable(columnIndex);
        }
        return slaveCursor.getSymbolTable(columnIndex - columnSplit);
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        if (columnIndex < columnSplit) {
            return masterCursor.newSymbolTable(columnIndex);
        }
        return slaveCursor.newSymbolTable(columnIndex - columnSplit);
    }
}
