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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.*;
import io.questdb.cairo.pool.AbstractMultiTenantPool;
import io.questdb.cairo.pool.ReaderPool;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;

import java.util.Iterator;
import java.util.Map;

public final class ReaderPoolRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final RecordMetadata METADATA;
    private static final int OWNER_COLUMN_INDEX = 1;
    private static final int TABLE_COLUMN_INDEX = 0;
    private static final int TIMESTAMP_COLUMN_INDEX = 2;
    private static final int TXN_COLUMN_INDEX = 3;
    private final CairoEngine cairoEngine;

    public ReaderPoolRecordCursorFactory(CairoEngine cairoEngine) {
        super(METADATA);
        this.cairoEngine = cairoEngine;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        ReaderPoolCursor readerPoolCursor = new ReaderPoolCursor();
        readerPoolCursor.of(cairoEngine.getReaderPoolEntries());
        return readerPoolCursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("reader_pool");
    }

    private static class ReaderPoolCursor implements RecordCursor {
        private final ReaderPoolEntryRecord record = new ReaderPoolEntryRecord();
        private int allocationIndex = 0;
        private Iterator<Map.Entry<CharSequence, AbstractMultiTenantPool.Entry<ReaderPool.R>>> iterator;
        private long owner;
        private AbstractMultiTenantPool.Entry<ReaderPool.R> poolEntry;
        private Map<CharSequence, AbstractMultiTenantPool.Entry<ReaderPool.R>> readerPoolEntries;
        private TableToken tableToken;
        private long timestamp;
        private long txn;

        @Override
        public void close() {
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public Record getRecordB() {
            throw new UnsupportedOperationException("RecordB not implemented");
        }

        @Override
        public boolean hasNext() {
            return tryAdvance();
        }

        public void of(Map<CharSequence, AbstractMultiTenantPool.Entry<ReaderPool.R>> readerPoolEntries) {
            this.readerPoolEntries = readerPoolEntries;
            toTop();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException("Random access not implemented");
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            iterator = readerPoolEntries.entrySet().iterator();
            allocationIndex = 0;
            poolEntry = null;
        }

        private boolean selectPoolEntry() {
            for (; ; ) {
                if (poolEntry == null) {
                    // either we just started the iteration or the last Entry did not have
                    // anything chained. let's advance in the CHM iterator
                    assert allocationIndex == 0;
                    if (!iterator.hasNext()) {
                        return false;
                    }
                    Map.Entry<CharSequence, AbstractMultiTenantPool.Entry<ReaderPool.R>> mapEntry = iterator.next();
                    poolEntry = mapEntry.getValue();
                    return true;
                } else if (allocationIndex == ReaderPool.ENTRY_SIZE) {
                    // we exhausted all slots in the current Entry
                    // let's see if there is another Entry chained
                    poolEntry = poolEntry.getNext();
                    allocationIndex = 0;
                } else {
                    // the current entry still has slots to inspect
                    return true;
                }
            }
        }

        private boolean tryAdvance() {
            TableReader reader;
            do {
                if (!selectPoolEntry()) {
                    return false;
                }
                // Volatile read because the owner is set by other thread(s).
                // It ensures everything the other thread wrote to other fields before (volatile) writing the 'owner'
                // will become visible too (Synchronizes-With Order).
                // This does not imply the individual fields will be consistent. Why? There are other threads modifying
                // entries concurrently with us reading them. So e.g. the 'reader' field can be changed after we read
                // 'owner'. So what's the point of volatile read? It ensures we don't read arbitrary stale data.
                owner = poolEntry.getOwnerVolatile(allocationIndex);
                timestamp = poolEntry.getReleaseOrAcquireTime(allocationIndex);
                reader = poolEntry.getTenant(allocationIndex);
                allocationIndex++;
            } while (reader == null);
            txn = reader.getTxn();
            tableToken = reader.getTableToken();
            return true;
        }

        private class ReaderPoolEntryRecord implements Record {
            @Override
            public long getLong(int col) {
                switch (col) {
                    case OWNER_COLUMN_INDEX:
                        return owner;
                    case TXN_COLUMN_INDEX:
                        return txn;
                    default:
                        throw CairoException.nonCritical().put("unsupported column number. [column=").put(col).put("]");
                }
            }

            @Override
            public CharSequence getStr(int col) {
                assert col == TABLE_COLUMN_INDEX;
                return tableToken.getTableName();
            }

            @Override
            public CharSequence getStrB(int col) {
                // CharSequence are mutable, but in this case CharSequences were used as map keys
                // hence I don't assume they would be mutable
                // todo: explore what happens when a table is renamed
                return getStr(col);
            }

            @Override
            public long getTimestamp(int col) {
                assert col == TIMESTAMP_COLUMN_INDEX;
                return timestamp;
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(TABLE_COLUMN_INDEX, new TableColumnMetadata("table", ColumnType.STRING))
                .add(OWNER_COLUMN_INDEX, new TableColumnMetadata("owner", ColumnType.LONG))
                .add(TIMESTAMP_COLUMN_INDEX, new TableColumnMetadata("timestamp", ColumnType.TIMESTAMP))
                .add(TXN_COLUMN_INDEX, new TableColumnMetadata("txn", ColumnType.LONG));
        METADATA = metadata;
    }
}
