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

package com.nfsdb.lang.cst.impl.rsrc;

import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.lang.cst.*;
import com.nfsdb.lang.cst.impl.ref.StringRef;
import com.nfsdb.storage.KVIndex;

public class KvIndexTopRowSource implements RowSource, RowCursor {

    private final StringRef column;
    private final RowFilter filter;
    private final KeySource keySource;

    private KVIndex index;
    private KeyCursor keyCursor;
    private long lo;
    private long hi;
    private KVIndex.IndexCursor indexCursor;
    private long localRowID;
    private RowAcceptor rowAcceptor;

    public KvIndexTopRowSource(StringRef column, KeySource keySource, RowFilter filter) {
        this.column = column;
        this.keySource = keySource;
        this.filter = filter;
    }

    @Override
    public RowCursor cursor(PartitionSlice slice) {
        rowAcceptor = filter != null ? filter.acceptor(slice) : null;
        try {
            this.index = slice.partition.getIndexForColumn(column.value);
            this.lo = slice.lo;
            this.hi = slice.calcHi ? slice.partition.open().size() - 1 : slice.hi;
            this.keyCursor = keySource.cursor(slice);
            return this;
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
    }

    @Override
    public void reset() {
        keySource.reset();
        indexCursor = null;
    }

    @Override
    public boolean hasNext() {

        if (!keyCursor.hasNext()) {
            return false;
        }

        indexCursor = index.cachedCursor(keyCursor.next());
        while (indexCursor.hasNext()) {
            localRowID = indexCursor.next();
            if (localRowID >= lo && localRowID <= hi && (rowAcceptor == null || rowAcceptor.accept(localRowID) == Choice.PICK)) {
                return true;
            }

            if (localRowID < lo) {
                break;
            }
        }

        return false;
    }

    @Override
    public long next() {
        return localRowID;
    }
}
