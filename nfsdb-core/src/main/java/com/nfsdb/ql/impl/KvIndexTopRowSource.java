/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.ql.impl;

import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.ql.KeyCursor;
import com.nfsdb.ql.KeySource;
import com.nfsdb.ql.PartitionSlice;
import com.nfsdb.ql.RowCursor;
import com.nfsdb.ql.ops.VirtualColumn;
import com.nfsdb.storage.IndexCursor;
import com.nfsdb.storage.KVIndex;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;


/**
 * Streams rowids on assumption that {@link #keySource} produces only one key.
 * This is used in nested-loop join where "slave" source is scanned for one key at a time.
 */
@SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CHECKED"})
public class KvIndexTopRowSource extends AbstractRowSource {

    private final String column;
    private final VirtualColumn filter;
    private final KeySource keySource;
    private JournalRecord rec;

    private KVIndex index;
    private KeyCursor keyCursor;
    private long lo;
    private long hi;

    public KvIndexTopRowSource(String column, KeySource keySource, VirtualColumn filter) {
        this.column = column;
        this.keySource = keySource;
        this.filter = filter;
    }

    @Override
    public void configure(JournalMetadata metadata) {
        this.rec = new JournalRecord(metadata);
    }

    @Override
    public RowCursor prepareCursor(PartitionSlice slice) {
        try {
            this.index = slice.partition.getIndexForColumn(column);
            this.lo = slice.lo - 1;
            this.hi = slice.calcHi ? slice.partition.open().size() : slice.hi + 1;
            this.keyCursor = keySource.prepareCursor();
            this.rec.partition = slice.partition;
            return this;
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
    }

    @Override
    public void reset() {
        keySource.reset();
    }

    @Override
    public boolean hasNext() {

        if (!keyCursor.hasNext()) {
            return false;
        }

        IndexCursor indexCursor = index.cursor(keyCursor.next());
        while (indexCursor.hasNext()) {
            rec.rowid = indexCursor.next();
            if (rec.rowid > lo && rec.rowid < hi && (filter == null || filter.getBool(rec))) {
                return true;
            }

            if (rec.rowid <= lo) {
                break;
            }
        }

        return false;
    }

    @Override
    public long next() {
        return rec.rowid;
    }
}
