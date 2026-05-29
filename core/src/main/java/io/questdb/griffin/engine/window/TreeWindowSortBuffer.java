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

package io.questdb.griffin.engine.window;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.orderby.LongTreeChain;
import io.questdb.griffin.engine.orderby.SortKeyEncoder;
import io.questdb.std.DirectIntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

final class TreeWindowSortBuffer implements WindowSortBuffer {
    private final RecordComparator comparator;
    private final ObjList<DirectIntList> rankMaps;
    private final LongTreeChain tree;
    private Record chainRightRecord;
    private LongTreeChain.TreeCursor cursor;
    private RecordCursor sourceCursor;

    /**
     * Takes ownership of {@code rankMaps}: frees the inner DirectIntList contents
     * if the inner tree allocation fails, and on {@link #close()}. Callers must not
     * retain a separate reference.
     */
    TreeWindowSortBuffer(
            CairoConfiguration configuration,
            RecordComparator comparator,
            ObjList<DirectIntList> rankMaps
    ) {
        this.comparator = comparator;
        this.rankMaps = rankMaps;
        try {
            this.tree = new LongTreeChain(
                    configuration.getSqlWindowTreeKeyPageSize(),
                    configuration.getSqlWindowTreeKeyMaxPages(),
                    configuration.getSqlWindowRowIdPageSize(),
                    configuration.getSqlWindowRowIdMaxPages()
            );
        } catch (Throwable th) {
            Misc.freeObjListAndKeepObjects(rankMaps);
            throw th;
        }
    }

    @Override
    public void close() {
        Misc.free(tree);
        if (rankMaps != null) {
            Misc.freeObjListAndKeepObjects(rankMaps);
        }
        cursor = null;
        chainRightRecord = null;
        sourceCursor = null;
    }

    @Override
    public void finishPut(SqlExecutionCircuitBreaker circuitBreaker) {
    }

    @Override
    public boolean hasNext() {
        return cursor != null && cursor.hasNext();
    }

    @Override
    public long next() {
        return cursor.next();
    }

    @Override
    public void of(RecordCursor cursor, long expectedRows) {
        this.sourceCursor = cursor;
        this.chainRightRecord = cursor.getRecordB();
        SortKeyEncoder.buildRankMaps(cursor, rankMaps, comparator);
    }

    @Override
    public void put(Record record, long rowId) {
        tree.put(record, sourceCursor, chainRightRecord, comparator, rowId);
    }

    @Override
    public void reopen() {
        tree.reopen();
    }

    @Override
    public void toTop() {
        cursor = tree.getCursor();
    }
}
