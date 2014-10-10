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

package com.nfsdb.journal.lang.cst.impl.rsrc;

import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.index.KVIndex;
import com.nfsdb.journal.lang.cst.*;
import com.nfsdb.journal.lang.cst.impl.ref.StringRef;

public class KvIndexHeadRowSource implements RowSource, RowCursor {

    private final StringRef column;
    private final KeySource keySource;
    private final int count;
    private final int tailOffset;
    private final RowFilter filter;

    // state
    private int remainingKeys[];
    private int remainingCounts[];
    private int remainingOffsets[];
    private KVIndex index;
    private long lo;
    private long hi;
    private int keyIndex;
    private KVIndex.IndexCursor indexCursor;
    private long localRowID;
    private int keyCount = -1;
    private RowAcceptor rowAcceptor;

    public KvIndexHeadRowSource(StringRef column, KeySource keySource, int count, int tailOffset, RowFilter filter) {
        this.column = column;
        this.keySource = keySource;
        this.count = count;
        this.tailOffset = tailOffset;
        this.filter = filter;
    }

    @Override
    public RowCursor cursor(PartitionSlice slice) {
        if (keyCount == -1) {

            KeyCursor cursor = keySource.cursor(slice);

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

        rowAcceptor = filter != null ? filter.acceptor(slice) : null;

        try {
            this.index = slice.partition.getIndexForColumn(column.value);
            this.lo = slice.lo;
            this.hi = slice.calcHi ? slice.partition.open().size() - 1 : slice.hi;
            this.keyIndex = 0;
            return this;
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
    }

    @Override
    public boolean hasNext() {
        int cnt;
        int o;
        if (indexCursor != null && (cnt = remainingCounts[keyIndex]) > 0 && indexCursor.hasNext()) {
            localRowID = indexCursor.next();
            if (localRowID >= lo && localRowID <= hi && (rowAcceptor == null || rowAcceptor.accept(localRowID) == Choice.PICK)) {
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
        return localRowID;
    }

    @Override
    public void reset() {
        keyCount = -1;
        keySource.reset();
        indexCursor = null;
    }

    private boolean hasNextKey() {
        while (this.keyIndex < keyCount) {

            // running first time for keyIndex?
            if (indexCursor == null) {
                indexCursor = index.cachedCursor(remainingKeys[this.keyIndex]);
            }

            int o = remainingOffsets[keyIndex];
            int cnt = remainingCounts[keyIndex];
            if (cnt > 0) {
                while (indexCursor.hasNext()) {
                    if ((localRowID = indexCursor.next()) < lo) {
                        break;
                    }
                    // this is a good rowid
                    if (localRowID <= hi && (rowAcceptor == null || rowAcceptor.accept(localRowID) == Choice.PICK)) {
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
