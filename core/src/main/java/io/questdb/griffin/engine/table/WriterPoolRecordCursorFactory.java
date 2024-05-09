/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.*;
import io.questdb.cairo.pool.WriterPool;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Numbers;

import java.util.Iterator;
import java.util.Map;

public final class WriterPoolRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final RecordMetadata METADATA;
    private static final int OWNER_THREAD_COLUMN_INDEX = 1;
    private static final int TABLE_NAME_COLUMN_INDEX = 0;
    private static final int LAST_ACCESS_TIMESTAMP_COLUMN_INDEX = 2;
    private static final int OWNERSHIP_REASON_COLUMN_INDEX = 3;
    private final CairoEngine cairoEngine;

    public WriterPoolRecordCursorFactory(CairoEngine cairoEngine) {
        super(METADATA);
        this.cairoEngine = cairoEngine;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        WriterPoolCursor writerPoolCursor = new WriterPoolCursor();
        writerPoolCursor.of(cairoEngine.getWriterPoolEntries());
        return writerPoolCursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("writer_pool");
    }

    private static class WriterPoolCursor implements NoRandomAccessRecordCursor {
        private final ReaderPoolEntryRecord record = new ReaderPoolEntryRecord();
        private Iterator<Map.Entry<CharSequence, WriterPool.Entry>> iterator;
        private long owner_thread;
        private Map<CharSequence, WriterPool.Entry> writerPoolEntries;
        private TableToken tableToken;
        private String ownershipReason;
        private long lastAccessTimestamp;
        @Override
        public void close() {
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            if (iterator.hasNext()) {
                Map.Entry<CharSequence, WriterPool.Entry> mapEntry = iterator.next();
                final WriterPool.Entry poolEntry = mapEntry.getValue();
                owner_thread = poolEntry.getOwnerThread();
                lastAccessTimestamp = poolEntry.getLastReleaseTime();
                tableToken = poolEntry.getTableToken();
                ownershipReason = poolEntry.getOwnershipReason();
                return true;
            }
            return false;
        }

        public void of(Map<CharSequence, WriterPool.Entry> readerPoolEntries) {
            this.writerPoolEntries = readerPoolEntries;
            toTop();
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            iterator = writerPoolEntries.entrySet().iterator();
        }

        private class ReaderPoolEntryRecord implements Record {
            @Override
            public long getLong(int col) {
                if (col == OWNER_THREAD_COLUMN_INDEX) {
                    return owner_thread == -1 ? Numbers.LONG_NULL : owner_thread;
                }
                throw CairoException.nonCritical().put("unsupported column number. [column=").put(col).put("]");
            }

            @Override
            public CharSequence getStrA(int col) {
                switch (col) {
                    case TABLE_NAME_COLUMN_INDEX:
                        return tableToken.getTableName();
                    case OWNERSHIP_REASON_COLUMN_INDEX:
                        return ownershipReason;
                    default:
                        throw CairoException.nonCritical().put("unsupported column number. [column=").put(col).put("]");
                }
            }

            @Override
            public CharSequence getStrB(int col) {
                return getStrA(col);
            }

            @Override
            public long getTimestamp(int col) {
                assert col == LAST_ACCESS_TIMESTAMP_COLUMN_INDEX;
                return lastAccessTimestamp;
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(TABLE_NAME_COLUMN_INDEX, new TableColumnMetadata("table_name", ColumnType.STRING))
                .add(OWNER_THREAD_COLUMN_INDEX, new TableColumnMetadata("owner_thread_id", ColumnType.LONG))
                .add(LAST_ACCESS_TIMESTAMP_COLUMN_INDEX, new TableColumnMetadata("last_access_timestamp", ColumnType.TIMESTAMP))
                .add(OWNERSHIP_REASON_COLUMN_INDEX, new TableColumnMetadata("ownership_reason", ColumnType.STRING))
        ;
        METADATA = metadata;
    }
}
