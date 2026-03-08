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

package io.questdb.griffin.engine.union;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlException;
import io.questdb.std.Misc;

public abstract class AbstractSetRecordCursor implements RecordCursor {
    protected SqlExecutionCircuitBreaker circuitBreaker;
    protected RecordCursor cursorA;
    protected RecordCursor cursorB;

    @Override
    public void close() {
        this.cursorA = Misc.free(this.cursorA);
        this.cursorB = Misc.free(this.cursorB);
        this.circuitBreaker = null;
    }

    void of(RecordCursor cursorA, RecordCursor cursorB, SqlExecutionCircuitBreaker circuitBreaker) throws SqlException {
        this.cursorA = cursorA;
        this.cursorB = cursorB;
        this.circuitBreaker = circuitBreaker;
    }
}
