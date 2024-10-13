/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.AbstractVirtualFunctionRecordCursor;
import io.questdb.std.ObjList;

public class VirtualFunctionDirectSymbolRecordCursor extends AbstractVirtualFunctionRecordCursor {
    public VirtualFunctionDirectSymbolRecordCursor(ObjList<Function> functions, boolean supportsRandomAccess) {
        super(functions, supportsRandomAccess);
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, RecordCursor.Counter counter) {
        if (baseCursor != null) {
            baseCursor.calculateSize(circuitBreaker, counter);
        } else {
            RecordCursor.calculateSize(this, circuitBreaker, counter);
        }
    }

    @Override
    public void skipRows(Counter rowCount) throws DataUnavailableException {
        if (baseCursor != null) {
            baseCursor.skipRows(rowCount);
        } else {
            RecordCursor.skipRows(this, rowCount);
        }
    }
}
