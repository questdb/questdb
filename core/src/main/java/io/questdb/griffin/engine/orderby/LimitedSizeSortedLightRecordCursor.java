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

package io.questdb.griffin.engine.orderby;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.std.Misc;

/**
 * SortedLightRecordCursor which implements LIMIT clause.
 */
public class LimitedSizeSortedLightRecordCursor implements DelegatingRecordCursor {

    private final LimitedSizeLongTreeChain chain;
    private final LimitedSizeLongTreeChain.TreeCursor chainCursor;
    private final RecordComparator comparator;
    private final long limit; // <0 - limit disabled; =0 means don't fetch any rows; >0 - apply limit
    private final long skipFirst; // skip first N rows
    private final long skipLast;  // skip last N rows
    private RecordCursor base;
    private Record baseRecord;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private boolean isChainBuilt;
    private boolean isOpen;
    private long rowsLeft;

    public LimitedSizeSortedLightRecordCursor(
            LimitedSizeLongTreeChain chain,
            RecordComparator comparator,
            long limit,
            long skipFirst,
            long skipLast
    ) {
        this.chain = chain;
        this.comparator = comparator;
        this.chainCursor = chain.getCursor();
        this.limit = limit;
        this.skipFirst = skipFirst;
        this.skipLast = skipLast;
        this.isOpen = true;
    }

    @Override
    public void close() {
        if (isOpen) {
            Misc.free(chain);
            Misc.free(base);
            isOpen = false;
        }
    }

    @Override
    public Record getRecord() {
        return baseRecord;
    }

    @Override
    public Record getRecordB() {
        return base.getRecordB();
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return base.getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (!isChainBuilt) {
            buildChain();
            isChainBuilt = true;
        }
        if (rowsLeft-- > 0 && chainCursor.hasNext()) {
            base.recordAt(baseRecord, chainCursor.next());
            return true;
        }
        return false;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return base.newSymbolTable(columnIndex);
    }

    @Override
    public void of(RecordCursor base, SqlExecutionContext executionContext) {
        if (!isOpen) {
            chain.reopen();
            isOpen = true;
        }

        this.base = base;
        baseRecord = base.getRecord();
        circuitBreaker = executionContext.getCircuitBreaker();
        isChainBuilt = false;
        chain.clear();
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        base.recordAt(record, atRowId);
    }

    @Override
    public long size() {
        return isChainBuilt ? Math.max(chain.size() - skipFirst - skipLast, 0) : -1;
    }

    @Override
    public void toTop() {
        chainCursor.toTop();

        long skipLeft = skipFirst;
        while (skipLeft-- > 0 && chainCursor.hasNext()) {
            chainCursor.next();
        }

        rowsLeft = Math.max(chain.size() - skipFirst - skipLast, 0);
    }

    private void buildChain() {
        final Record placeHolderRecord = base.getRecordB();
        if (limit != 0) {
            while (base.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
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
        }
        toTop();
    }
}

