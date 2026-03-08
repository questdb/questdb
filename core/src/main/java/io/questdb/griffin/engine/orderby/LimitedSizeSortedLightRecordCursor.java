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
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.std.Misc;

/**
 * SortedLightRecordCursor which implements LIMIT clause.
 */
public class LimitedSizeSortedLightRecordCursor implements DelegatingRecordCursor, DynamicLimitCursor {
    private final LimitedSizeLongTreeChain chain;
    private final LimitedSizeLongTreeChain.TreeCursor chainCursor;
    private final RecordComparator comparator;
    private RecordCursor baseCursor;
    private Record baseRecord;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private boolean isChainBuilt;
    private boolean isOpen;
    private long limit; // <0 - limit disabled; =0 means don't fetch any rows; >0 - apply limit
    private long rowsLeft;
    private long skipFirst; // skip first N rows
    private long skipLast;  // skip last N rows

    public LimitedSizeSortedLightRecordCursor(
            LimitedSizeLongTreeChain chain,
            RecordComparator comparator
    ) {
        this.chain = chain;
        this.comparator = comparator;
        this.chainCursor = chain.getCursor();
        this.isOpen = true;
    }

    @Override
    public void close() {
        if (isOpen) {
            baseCursor = Misc.free(baseCursor);
            Misc.free(chain);
            isOpen = false;
        }
    }

    @Override
    public Record getRecord() {
        return baseRecord;
    }

    @Override
    public Record getRecordB() {
        return baseCursor.getRecordB();
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return baseCursor.getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (!isChainBuilt) {
            buildChain();
            isChainBuilt = true;
        }
        if (rowsLeft-- > 0 && chainCursor.hasNext()) {
            baseCursor.recordAt(baseRecord, chainCursor.next());
            return true;
        }
        return false;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return baseCursor.newSymbolTable(columnIndex);
    }

    @Override
    public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) {
        this.baseCursor = baseCursor;
        baseRecord = baseCursor.getRecord();
        if (!isOpen) {
            isOpen = true;
            chain.reopen();
        }
        circuitBreaker = executionContext.getCircuitBreaker();
        isChainBuilt = false;
        chain.clear();
    }

    @Override
    public long preComputedStateSize() {
        return RecordCursor.fromBool(isChainBuilt) + baseCursor.preComputedStateSize();
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        baseCursor.recordAt(record, atRowId);
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

    @Override
    public void updateLimits(long limit, long skipFirst, long skipLast) {
        this.limit = limit;
        this.skipFirst = skipFirst;
        this.skipLast = skipLast;
    }

    private void buildChain() {
        final Record placeHolderRecord = baseCursor.getRecordB();
        if (limit != 0) {
            while (baseCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                // Tree chain is liable to re-position record to
                // other rows to do record comparison. We must use our
                // own record instance in case base cursor keeps
                // state in the record it returns.
                chain.put(
                        baseRecord,
                        baseCursor,
                        placeHolderRecord,
                        comparator
                );
            }
        }
        toTop();
    }
}
