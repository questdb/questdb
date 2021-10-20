/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionInterruptor;
import io.questdb.griffin.engine.RecordComparator;

class SortedLightRecordCursor implements DelegatingRecordCursor {
    private final LongTreeChain chain;
    private final RecordComparator comparator;
    private final LongTreeChain.TreeCursor chainCursor;
    private RecordCursor base;
    private Record baseRecord;

    public SortedLightRecordCursor(LongTreeChain chain, RecordComparator comparator) {
        this.chain = chain;
        this.comparator = comparator;
        // assign it once, its the same instance anyway
        this.chainCursor = chain.getCursor();
    }

    @Override
    public void close() {
        chain.clear();
        base.close();
    }

    @Override
    public long size() {
        return base.size();
    }

    @Override
    public Record getRecord() {
        return baseRecord;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return base.getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (chainCursor.hasNext()) {
            base.recordAt(baseRecord, chainCursor.next());
            return true;
        }
        return false;
    }

    @Override
    public Record getRecordB() {
        return base.getRecordB();
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        base.recordAt(record, atRowId);
    }

    @Override
    public void toTop() {
        chainCursor.toTop();
    }

    @Override
    public void of(RecordCursor base, SqlExecutionContext executionContext) {
        this.base = base;
        this.baseRecord = base.getRecord();
        final Record placeHolderRecord = base.getRecordB();
        SqlExecutionInterruptor interruptor = executionContext.getSqlExecutionInterruptor();

        chain.clear();
        while (base.hasNext()) {
            interruptor.checkInterrupted();
            // Tree chain is liable to re-position record to
            // other rows to do record comparison. We must use our
            // own record instance in case base cursor keeps
            // state in the record it returns.
            chain.put(
                    baseRecord,
                    base,
                    placeHolderRecord,
                    comparator
            );
        }
        chainCursor.toTop();
    }
}
