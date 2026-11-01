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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.EmptyTableRandomRecordCursor;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

/**
 * The empty factory the generator builds in place of a factory whose rows it proves empty, when that factory holds
 * decorrelation domains other consumers read through shared cursors. It keeps the factory it replaces, which owns the
 * domains, and opens each domain as their primary whenever it opens, and closes them when it closes, so that a shared
 * cursor of a domain never opens, and then closes, the primary under the other consumers still reading it.
 */
public class DomainKeepingEmptyRecordCursorFactory extends EmptyTableRecordCursorFactory {
    private final DomainKeepingCursor cursor = new DomainKeepingCursor();
    private final ObjList<RecordCursorFactory> domains;
    private RecordCursorFactory kept;

    /**
     * Takes ownership of {@code kept}, which holds the {@code domains}, once constructed.
     */
    public DomainKeepingEmptyRecordCursorFactory(RecordMetadata metadata, RecordCursorFactory kept, ObjList<RecordCursorFactory> domains) {
        super(metadata);
        this.kept = kept;
        this.domains = domains;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        super.getCursor(executionContext);
        cursor.open(domains, executionContext);
        return cursor;
    }

    @Override
    protected void _close() {
        super._close();
        kept = Misc.free(kept);
    }

    private static class DomainKeepingCursor implements RecordCursor {
        private final ObjList<RecordCursor> domainCursors = new ObjList<>();

        @Override
        public void close() {
            Misc.freeObjListAndClear(domainCursors);
        }

        @Override
        public Record getRecord() {
            return EmptyTableRandomRecordCursor.INSTANCE.getRecord();
        }

        @Override
        public Record getRecordB() {
            return EmptyTableRandomRecordCursor.INSTANCE.getRecordB();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return EmptyTableRandomRecordCursor.INSTANCE.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return EmptyTableRandomRecordCursor.INSTANCE.newSymbolTable(columnIndex);
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public void toTop() {
        }

        void open(ObjList<RecordCursorFactory> domains, SqlExecutionContext executionContext) throws SqlException {
            try {
                for (int i = 0, n = domains.size(); i < n; i++) {
                    domainCursors.add(domains.getQuick(i).getCursor(executionContext));
                }
            } catch (Throwable th) {
                close();
                throw th;
            }
        }
    }
}
