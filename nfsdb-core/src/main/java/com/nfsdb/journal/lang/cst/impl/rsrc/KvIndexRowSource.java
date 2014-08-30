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
import com.nfsdb.journal.index.Cursor;
import com.nfsdb.journal.index.KVIndex;
import com.nfsdb.journal.lang.cst.*;
import com.nfsdb.journal.lang.cst.impl.ref.StringRef;

public class KvIndexRowSource implements RowSource, RowCursor {

    private final StringRef symbol;
    private final KeySource keySource;
    private KVIndex index;
    private Cursor indexCursor;
    private KeyCursor keyCursor;

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

        return indexCursor.hasNext();
    }

    @Override
    public long next() {
        return indexCursor.next();
    }

    @Override
    public void reset() {
        keySource.reset();
    }
}
