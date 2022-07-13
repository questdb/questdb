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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.*;
import io.questdb.cairo.pool.ReaderPool;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;

import java.util.Iterator;
import java.util.Map;

public final class ReaderPoolRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final RecordMetadata METADATA;

    private final CairoEngine cairoEngine;

    public ReaderPoolRecordCursorFactory(CairoEngine cairoEngine) {
        super(METADATA);
        this.cairoEngine = cairoEngine;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        return new ReaderPoolCursor();
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("table", 1, ColumnType.STRING))
                .add(new TableColumnMetadata("owner", 1, ColumnType.LONG))
                .add(new TableColumnMetadata("timestamp", 1, ColumnType.TIMESTAMP))
                .add(new TableColumnMetadata("txn", 1, ColumnType.LONG));
        METADATA = metadata;
    }

    private class ReaderPoolCursor implements RecordCursor {
        private Iterator<Map.Entry<CharSequence, ReaderPool.Entry>> iterator;
        private ReaderPool.Entry poolEntry;
        private int allocationIndex = 0;
        private CharSequence tableName;
        private long owner;
        private long timestamp;
        private long txn;
        private final ReaderPoolEntryRecord record = new ReaderPoolEntryRecord();

        public ReaderPoolCursor() {
            of();
        }


        public void of() {
            allocationIndex = 0;
            poolEntry = null;
            tableName = null;
            Map<CharSequence, ReaderPool.Entry> readerPoolEntries = cairoEngine.getReaderPoolEntries();
            iterator = readerPoolEntries.entrySet().iterator();
        }

        private boolean tryAdvance() {
            TableReader reader;
            do {
                if (!selectPoolEntry()) {
                    return false;
                }
                owner = poolEntry.getOwnerVolatile(allocationIndex);
                timestamp = poolEntry.getReleaseOrAcquireTimeVolatile(allocationIndex);
                reader = poolEntry.getReaderVolatile(allocationIndex);
                allocationIndex++;
            } while (reader == null);
            txn = reader.getTxnVolatile();
            return true;
        }

        private boolean selectPoolEntry() {
            for (;;) {
                if (poolEntry == null) {
                    // either we just started the iteration or the last Entry did not have
                    // anything chained. let's advance in the CHM iterator
                    assert allocationIndex == 0;
                    if (!iterator.hasNext()) {
                        return false;
                    }
                    Map.Entry<CharSequence, ReaderPool.Entry> mapEntry = iterator.next();
                    tableName = mapEntry.getKey();
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

        @Override
        public void close() {
            //todo
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            return tryAdvance();
        }

        @Override
        public Record getRecordB() {
            throw new UnsupportedOperationException("RecordB not implemented");
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException("Random access not implemented");
        }

        @Override
        public void toTop() {
            of();
        }

        @Override
        public long size() {
            return -1;
        }

        private class ReaderPoolEntryRecord implements Record {
            @Override
            public CharSequence getStr(int col) {
                assert col == 0;
                return tableName;
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
                assert col == 2;
                return timestamp;
            }

            @Override
            public long getLong(int col) {
                if (col == 1) {
                    return owner;
                } else if (col == 3) {
                    return txn;
                } else {
                    throw CairoException.instance(0).put("unsupported column number. [column=").put(col).put("]");
                }
            }
        }
    }
}
