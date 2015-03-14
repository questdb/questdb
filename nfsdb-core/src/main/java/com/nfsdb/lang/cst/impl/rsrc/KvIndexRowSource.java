/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.lang.cst.impl.rsrc;

import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.lang.cst.*;
import com.nfsdb.lang.cst.impl.ref.StringRef;
import com.nfsdb.storage.Cursor;
import com.nfsdb.storage.KVIndex;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CHECKED"})
public class KvIndexRowSource implements RowSource, RowCursor {

    private final StringRef symbol;
    private final KeySource keySource;
    private KVIndex index;
    private Cursor indexCursor;
    private KeyCursor keyCursor;
    private long lo;
    private long hi;
    private boolean full;
    private long rowid;

    public KvIndexRowSource(StringRef symbol, KeySource keySource) {
        this.symbol = symbol;
        this.keySource = keySource;
    }

    @Override
    public RowCursor cursor(PartitionSlice slice) {
        try {
            this.index = slice.partition.getIndexForColumn(symbol.value);
            this.keyCursor = this.keySource.cursor(slice);
            this.indexCursor = null;
            this.full = slice.lo == 0 && slice.calcHi;
            this.lo = slice.lo;
            this.hi = slice.calcHi ? slice.partition.open().size() - 1 : slice.hi;
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
        return this;
    }

    @Override
    public boolean hasNext() {

        if (indexCursor == null && !keyCursor.hasNext()) {
            return false;
        }

        if (indexCursor == null) {
            this.indexCursor = index.cachedCursor(keyCursor.next());
        }

        if (full) {
            return indexCursor.hasNext();
        } else {
            while (indexCursor.hasNext()) {
                long rowid = indexCursor.next();
                if (rowid >= lo && rowid <= hi) {
                    this.rowid = rowid;
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public long next() {
        return full ? indexCursor.next() : this.rowid;
    }

    @Override
    public void reset() {
        keySource.reset();
    }
}
