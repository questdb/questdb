/*+*****************************************************************************
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

package io.questdb.griffin.engine;

import io.questdb.cairo.RecordArray;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;

/**
 * The executable form of one sub-query of a statement: the single factory the generator builds for it, which every
 * consumer reads through its own {@link SubqueryRecordCursorFactory}. With one consumer the rows stream from the
 * factory to that consumer. With more, {@link #share(RecordArray)} gives the sub-query native row storage: the first
 * consumer to read it in an execution materialises the rows, later consumers of the same execution read them, and
 * {@link SubqueryScopeRecordCursorFactory} releases them when the execution starts and ends. The base cursor stays
 * open while the rows are materialised, because the rows resolve symbol keys through it.
 * <p>
 * Consumers reference the result; the last one to close frees the factory and the rows.
 */
public final class SubqueryResult implements QuietCloseable {
    private final RecordCursorFactory base;
    private RecordCursor baseCursor;
    private int consumerCount;
    private boolean isClosed;
    private boolean isMaterialized;
    private int referenceCount;
    private RecordArray rows;

    public SubqueryResult(RecordCursorFactory base) {
        this.base = base;
    }

    @Override
    public void close() {
        if (!isClosed) {
            isClosed = true;
            try {
                releaseRows();
            } finally {
                rows = Misc.free(rows);
                Misc.free(base);
            }
        }
    }

    public RecordCursorFactory getBase() {
        return base;
    }

    /**
     * The number of open consumers that evaluate the sub-query; worker clones, which inherit their owner's value,
     * are not counted.
     */
    public int getConsumerCount() {
        return consumerCount;
    }

    public boolean isClosed() {
        return isClosed;
    }

    public boolean isShared() {
        return rows != null;
    }

    /**
     * Releases the rows of the current execution and the base cursor they resolve symbols through; the row storage
     * stays allocated for the next execution.
     */
    public void releaseRows() {
        isMaterialized = false;
        try {
            baseCursor = Misc.free(baseCursor);
        } finally {
            if (rows != null) {
                rows.setSymbolTableResolver(null);
                rows.clear();
            }
        }
    }

    /**
     * Stores the rows of every execution in {@code rows} so that all consumers read one evaluation; the result owns
     * {@code rows} from the call on.
     */
    public void share(RecordArray rows) {
        assert this.rows == null && !isClosed;
        this.rows = rows;
    }

    void acquire(boolean isConsumer) {
        assert !isClosed;
        referenceCount++;
        if (isConsumer) {
            consumerCount++;
        }
    }

    RecordCursor getBaseCursor() {
        return baseCursor;
    }

    /**
     * The rows of the current execution, materialised on the first call of the execution.
     */
    RecordArray getRows(SqlExecutionContext executionContext) throws SqlException {
        if (!isMaterialized) {
            materialise(executionContext);
        }
        return rows;
    }

    void release(boolean isConsumer) {
        if (isConsumer) {
            consumerCount--;
        }
        if (--referenceCount == 0) {
            close();
        }
    }

    private void materialise(SqlExecutionContext executionContext) throws SqlException {
        try {
            rows.setMemoryTracker(executionContext.getMemoryTracker());
            baseCursor = base.getCursor(executionContext);
            final Record record = baseCursor.getRecord();
            final SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
            while (baseCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                rows.put(record);
            }
            rows.setSymbolTableResolver(baseCursor);
            isMaterialized = true;
        } catch (Throwable th) {
            releaseRows();
            throw th;
        }
    }
}
