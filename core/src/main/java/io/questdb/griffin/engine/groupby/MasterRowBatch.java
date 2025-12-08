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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.RecordArray;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;

/**
 * Buffer for a batch of materialized master row records.
 * <p>
 * Each MasterRowBatch holds up to BATCH_SIZE rows. Rows are materialized using
 * RecordArray for efficient storage and random access.
 */
public class MasterRowBatch implements Mutable, QuietCloseable {
    public static final int BATCH_SIZE = 100;

    private final RecordArray recordArray;
    private final LongList recordOffsets = new LongList(BATCH_SIZE);

    public MasterRowBatch(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordSink recordSink
    ) {
        this.recordArray = new RecordArray(
                metadata,
                recordSink,
                configuration.getSqlHashJoinValuePageSize(),
                configuration.getSqlHashJoinValueMaxPages()
        );
    }

    /**
     * Add a master row record to this batch.
     *
     * @param row the master row record to add
     * @return true if the row was added, false if batch is full
     */
    public boolean add(Record row) {
        if (recordOffsets.size() >= BATCH_SIZE) {
            return false;
        }
        long offset = recordArray.put(row);
        recordOffsets.add(offset);
        return true;
    }

    @Override
    public void clear() {
        recordArray.clear();
        recordOffsets.clear();
    }

    @Override
    public void close() {
        Misc.free(recordArray);
        recordOffsets.clear();
    }

    /**
     * Get the underlying RecordArray.
     */
    public RecordArray getRecordArray() {
        return recordArray;
    }

    /**
     * Get the underlying record offsets for direct access.
     */
    public LongList getRecordOffsets() {
        return recordOffsets;
    }

    /**
     * Get a record at the specified index.
     *
     * @param index row index (0 to size-1)
     * @return the master row record
     */
    public Record getRowAt(int index) {
        assert index >= 0 && index < recordOffsets.size() : "index out of bounds";
        long offset = recordOffsets.getQuick(index);
        return recordArray.getRecordAt(offset);
    }

    public boolean isFull() {
        return recordOffsets.size() >= BATCH_SIZE;
    }

    /**
     * Set the symbol table resolver for reading symbol columns as strings.
     * This must be called before reading records if any symbol columns need to be
     * resolved to their string values.
     *
     * @param resolver the symbol table source for resolving symbol int values to strings
     */
    public void setSymbolTableResolver(SymbolTableSource resolver) {
        recordArray.setSymbolTableResolver(resolver);
    }

    public int size() {
        return (int) recordArray.size();
    }
}
