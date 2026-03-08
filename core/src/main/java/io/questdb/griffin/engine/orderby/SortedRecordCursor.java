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

package io.questdb.griffin.engine.orderby;

import io.questdb.cairo.sql.DelegatingRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

class SortedRecordCursor implements DelegatingRecordCursor {
    private final RecordTreeChain chain;
    private RecordCursor baseCursor;
    private RecordTreeChain.TreeCursor chainCursor;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private boolean isChainBuilt;
    private boolean isOpen;

    public SortedRecordCursor(RecordTreeChain chain) {
        this.chain = chain;
        this.isOpen = true;
    }

    @Override
    public void close() {
        if (isOpen) {
            isOpen = false;
            chainCursor = Misc.free(chainCursor);
            baseCursor = Misc.free(baseCursor);
            Misc.free(chain);
        }
    }

    @Override
    public Record getRecord() {
        return chainCursor.getRecord();
    }

    @Override
    public Record getRecordB() {
        return chainCursor.getRecordB();
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return chainCursor.getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (!isChainBuilt) {
            buildChain();
            isChainBuilt = true;
        }
        return chainCursor.hasNext();
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return chainCursor.newSymbolTable(columnIndex);
    }

    @Override
    public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) {
        this.baseCursor = baseCursor;
        if (!isOpen) {
            isOpen = true;
            chain.reopen();
        }
        chainCursor = chain.getCursor(baseCursor);
        circuitBreaker = executionContext.getCircuitBreaker();
        isChainBuilt = false;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        chainCursor.recordAt(record, atRowId);
    }

    @Override
    public long size() {
        return baseCursor.size();
    }

    @Override
    public long preComputedStateSize() {
        return chain.size();
    }

    @Override
    public void toTop() {
        chainCursor.toTop();
    }

    private void buildChain() {
        final Record record = baseCursor.getRecord();
        while (baseCursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            // Tree chain is liable to re-position record to
            // other rows to do record comparison. We must use our
            // own record instance in case base cursor keeps
            // state in the record it returns.
            chain.put(record);
        }
        toTop();
    }
}
