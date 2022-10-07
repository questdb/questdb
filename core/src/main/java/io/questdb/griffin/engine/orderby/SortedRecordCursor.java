/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.orderby;

import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

class SortedRecordCursor implements DelegatingRecordCursor {
    private final RecordTreeChain chain;
    private RecordTreeChain.TreeCursor chainCursor;
    private boolean isOpen;

    public SortedRecordCursor(RecordTreeChain chain) {
        this.chain = chain;
        this.isOpen = true;
    }

    @Override
    public void close() {
        if (isOpen) {
            Misc.free(chainCursor);
            Misc.free(chain);
            isOpen = false;
        }
    }

    @Override
    public Record getRecord() {
        return chainCursor.getRecord();
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return chainCursor.getSymbolTable(columnIndex);
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return chainCursor.newSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        return chainCursor.hasNext();
    }

    @Override
    public Record getRecordB() {
        return chainCursor.getRecordB();
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        chainCursor.recordAt(record, atRowId);
    }

    @Override
    public void toTop() {
        chainCursor.toTop();
    }

    @Override
    public long size() {
        return chainCursor.size();
    }

    @Override
    public void of(RecordCursor base, SqlExecutionContext executionContext) {
        try {
            if (!isOpen) {
                this.chain.reopen();
                this.isOpen = true;
            }
            this.chainCursor = chain.getCursor(base);
            final Record record = base.getRecord();
            final SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();

            while (base.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                // Tree chain is liable to re-position record to
                // other rows to do record comparison. We must use our
                // own record instance in case base cursor keeps
                // state in the record it returns.
                chain.put(record);
            }
            chainCursor.toTop();
        } catch (Throwable ex) {
            base.close();
            throw ex;
        }
    }
}
