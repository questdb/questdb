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
import com.nfsdb.ql.*;
import com.nfsdb.ql.ops.VirtualColumn;
import com.nfsdb.storage.IndexCursor;
import com.nfsdb.storage.KVIndex;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CHECKED"})
public class KvIndexHeadRowSource extends AbstractRowSource implements RecordSourceState {

    private final String column;
    private final KeySource keySource;
    private final int count;
    private final int tailOffset;
    private final VirtualColumn filter;
    private JournalRecord rec;

    // state
    private int remainingKeys[];
    private int remainingCounts[];
    private int remainingOffsets[];
    private KVIndex index;
    private long lo;
    private long hi;
    private int keyIndex;
    private IndexCursor indexCursor;
    private int keyCount = -1;

    public KvIndexHeadRowSource(String column, KeySource keySource, int count, int tailOffset, VirtualColumn filter) {
        this.column = column;
        this.keySource = keySource;
        this.count = count;
        this.tailOffset = tailOffset;
        this.filter = filter;
    }

    @Override
    public void configure(JournalMetadata metadata) {
        this.rec = new JournalRecord(metadata);
        if (filter != null) {
            this.filter.configureSource(this);
        }
    }

    @Override
    public RowCursor prepareCursor(PartitionSlice slice) {
        if (keyCount == -1) {

            KeyCursor cursor = keySource.prepareCursor(slice);

            int len = keySource.size();
            if (remainingKeys == null || remainingKeys.length < len) {
                this.remainingKeys = new int[len];
                this.remainingCounts = new int[len];
                this.remainingOffsets = new int[len];
            }

            int k = 0;
            while (cursor.hasNext()) {
                remainingKeys[k] = cursor.next();
                remainingCounts[k] = count;
                remainingOffsets[k] = tailOffset;
                k++;
            }
            this.keyCount = remainingKeys.length;
        }

        try {
            this.rec.partition = slice.partition;
            this.index = slice.partition.getIndexForColumn(column);
            this.lo = slice.lo;
            this.hi = slice.calcHi ? slice.partition.open().size() : slice.hi + 1;
            this.keyIndex = 0;
            return this;
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
    }

    @Override
    public void unprepare() {
        keyCount = -1;
        keySource.unprepare();
        indexCursor = null;
    }

    @Override
    public Record currentRecord() {
        return rec;
    }

    @Override
    public boolean hasNext() {
        int cnt;
        int o;
        if (indexCursor != null && (cnt = remainingCounts[keyIndex]) > 0 && indexCursor.hasNext()) {
            rec.rowid = indexCursor.next();
            if (rec.rowid >= lo && rec.rowid < hi && (filter == null || filter.getBool())) {
                if ((o = remainingOffsets[keyIndex]) == 0) {
                    remainingCounts[keyIndex] = cnt - 1;
                    return true;
                } else {
                    remainingOffsets[keyIndex] = o - 1;
                }
            }
        }
        return hasNextKey();
    }

    @Override
    public long next() {
        return rec.rowid;
    }

    private boolean hasNextKey() {
        while (this.keyIndex < keyCount) {

            // running first time for keyIndex?
            if (indexCursor == null) {
                indexCursor = index.cursor(remainingKeys[this.keyIndex]);
            }

            int o = remainingOffsets[keyIndex];
            int cnt = remainingCounts[keyIndex];
            if (cnt > 0) {
                while (indexCursor.hasNext()) {
                    if ((rec.rowid = indexCursor.next()) < lo) {
                        break;
                    }
                    // this is a good rowid
                    if (rec.rowid < hi && (filter == null || filter.getBool())) {
                        if (o == 0) {
                            remainingCounts[keyIndex] = cnt - 1;
                            return true;
                        } else {
                            o--;
                        }
                    }
                }
            }

            if (cnt > 0) {
                this.remainingOffsets[keyIndex] = o;
            }
            this.keyIndex++;
            this.indexCursor = null;
        }

        return false;

    }
}
