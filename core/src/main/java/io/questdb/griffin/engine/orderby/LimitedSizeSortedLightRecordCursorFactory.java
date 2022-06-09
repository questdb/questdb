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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.AbstractRedBlackTree;
import io.questdb.griffin.engine.RecordComparator;

/**
 * Same as SortedLightRecordCursorFactory but using LimitedSizeLongTreeChain instead.
 */
public class LimitedSizeSortedLightRecordCursorFactory extends AbstractRecordCursorFactory {


    private final RecordCursorFactory base;

    private final CairoConfiguration configuration;
    private final RecordComparator comparator;
    private final Function loFunction;
    private final Function hiFunction;

    //initialization delayed to getCursor() because lo/hi need to be evaluated
    private AbstractRedBlackTree chain; //LimitedSizeLongTreeChain or LongTreeChain
    private DelegatingRecordCursor cursor;//LimitedSizeSortedLightRecordCursor or SortedLightRecordCursor

    public LimitedSizeSortedLightRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory base,
            RecordComparator comparator,
            Function loFunc,
            Function hiFunc
    ) {
        super(metadata);
        this.base = base;
        this.loFunction = loFunc;
        this.hiFunction = hiFunc;
        this.configuration = configuration;
        this.comparator = comparator;
    }

    @Override
    public void _close() {
        base.close();
        if (chain != null) {
            chain.close();
        }
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            initialize(executionContext, baseCursor);

            cursor.of(baseCursor, executionContext);
            return cursor;
        } catch (RuntimeException ex) {
            baseCursor.close();
            throw ex;
        }
    }

    private void initialize(SqlExecutionContext executionContext, RecordCursor baseCursor) throws SqlException {
        if (isInitialized()) {
            return;
        }

        if (canBeOptimized(baseCursor, executionContext)) {
            initializeLimitedSizeCursor(executionContext, baseCursor);
        } else {
            initializeUnlimitedSizeCursor();
        }
    }

    private void initializeUnlimitedSizeCursor() {
        this.chain = new LongTreeChain(
                configuration.getSqlSortKeyPageSize(),
                configuration.getSqlSortKeyMaxPages(),
                configuration
                        .getSqlSortLightValuePageSize(),
                configuration.getSqlSortLightValueMaxPages());
        this.cursor = new SortedLightRecordCursor((LongTreeChain) chain, comparator);
    }

    /*
     * 1. "limit L" means we only need to keep :
     *      L >=0 - first L records
     *      L < 0 - last L records
     * 2. "limit L, H" means we need to keep :
     *    L < 0          - last  L records (but skip last H records, if H >=0 then don't skip anything    )
     *    L >= 0, H >= 0 - first H records (but skip first L later, if H <= L then return empty set)
     *    L >= 0, H < 0  - we can't optimize this case (because it spans from record L-th from the beginning up to
     *                     H-th from the end, and we don't  ) and need to revert to default behavior -
     *                     produce the whole set and skip .
     *
     *  Similar to LimitRecordCursorFactory.LimitRecordCursor but doesn't check underlying count .
     */
    public void initializeLimitedSizeCursor(SqlExecutionContext executionContext, RecordCursor base) throws SqlException {
        loFunction.init(base, executionContext);
        if (hiFunction != null) {
            hiFunction.init(base, executionContext);
        }

        long skipFirst = 0, skipLast = 0, limit;
        boolean isFirstN = false;

        long lo = loFunction.getLong(null);
        if (lo < 0 && hiFunction == null) {
            // last N rows
            // lo is negative, -5 for example
            // if we have 12 records we need to skip 12-5 = 7
            // if we have 4 records = return all of them
            // set limit to return remaining rows
            limit = -lo;
        } else if (lo > -1 && hiFunction == null) {
            // first N rows
            isFirstN = true;
            limit = lo;
        } else {
            // at this stage we also have 'hi'
            long hi = hiFunction.getLong(null);
            if (lo < 0) {
                // right, here we are looking for something like -10,-5 five rows away from tail
                if (lo < hi) {
                    limit = -lo;
                    skipLast = Math.max(-hi, 0);
                    //}
                } else {
                    // this is invalid bottom range, for example -3, -10
                    limit = 0;//produce empty result
                }
            } else { //lo >= 0
                if (hi < 0) {
                    //if lo>=0 but hi<0 then we fall back to standard algorithm because we can't estimate result size
                    // (it's from lo up to end-hi so probably whole result anyway )
                    limit = -1;
                    skipFirst = lo;
                    skipLast = -hi;
                } else { //both lo and hi are positive
                    if (hi <= lo) {
                        limit = 0;//produce empty result
                    } else {
                        isFirstN = true;
                        limit = hi;
                        //but we've to skip to lo
                        skipFirst = lo;
                    }
                }
            }
        }

        this.chain = new LimitedSizeLongTreeChain(
                configuration.getSqlSortKeyPageSize(),
                configuration.getSqlSortKeyMaxPages(),
                configuration.getSqlSortLightValuePageSize(),
                configuration.getSqlSortLightValueMaxPages(),
                isFirstN,
                limit);

        this.cursor = new LimitedSizeSortedLightRecordCursor((LimitedSizeLongTreeChain) chain, comparator, limit, skipFirst, skipLast);
    }

    private boolean isInitialized() {
        return chain != null && cursor != null;
    }

    // Check if lo, hi is set and lo >=0 while hi < 0 (meaning - return whole result set except some rows at start and some at the end)
    // because such case can't really be optimized by topN/bottomN
    private boolean canBeOptimized(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
        loFunction.init(baseCursor, executionContext);
        if (hiFunction != null) {
            hiFunction.init(baseCursor, executionContext);
        }

        return !(loFunction.getLong(null) >= 0 &&
                hiFunction != null &&
                hiFunction.getLong(null) < 0);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public boolean implementsLimit() {
        return true;
    }
}

