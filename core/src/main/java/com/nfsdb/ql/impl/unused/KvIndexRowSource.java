/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.ql.impl.unused;

import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.ql.KeyCursor;
import com.nfsdb.ql.KeySource;
import com.nfsdb.ql.PartitionSlice;
import com.nfsdb.ql.RowCursor;
import com.nfsdb.ql.impl.AbstractRowSource;
import com.nfsdb.store.IndexCursor;
import com.nfsdb.store.KVIndex;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CHECKED"})
public class KvIndexRowSource extends AbstractRowSource {

    private final String symbol;
    private final KeySource keySource;
    private final boolean newCursor;
    private KVIndex index;
    private IndexCursor indexCursor;
    private KeyCursor keyCursor;
    private long lo;
    private long hi;
    private boolean full;
    private long rowid;
    private boolean hasNext = false;

    public KvIndexRowSource(String symbol, KeySource keySource) {
        this(symbol, keySource, false);
    }

    private KvIndexRowSource(String symbol, KeySource keySource, boolean newCursor) {
        this.symbol = symbol;
        this.keySource = keySource;
        this.newCursor = newCursor;
    }

    @Override
    public void configure(JournalMetadata metadata) {
    }

    @Override
    public RowCursor prepareCursor(PartitionSlice slice) {
        try {
            this.index = slice.partition.getIndexForColumn(symbol);
            this.keyCursor = this.keySource.prepareCursor();
            this.indexCursor = null;
            this.full = slice.lo == 0 && slice.calcHi;
            this.lo = slice.lo - 1;
            this.hi = slice.calcHi ? slice.partition.open().size() : slice.hi + 1;
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
        return this;
    }

    @Override
    public void reset() {
        keySource.reset();
    }

    @Override
    public boolean hasNext() {

        if (hasNext) {
            return true;
        }

        if (indexCursor != null && indexCursor.hasNext()) {
            if (full) {
                this.rowid = indexCursor.next();
                return hasNext = true;
            }

            do {
                long rowid = indexCursor.next();
                if (rowid > lo && rowid < hi) {
                    this.rowid = rowid;
                    return hasNext = true;
                }
            } while (indexCursor.hasNext());
        }

        return hasNext = hasNext0();
    }

    @Override
    public long next() {
        hasNext = false;
        return rowid;
    }

    private boolean hasNext0() {
        while (keyCursor.hasNext()) {
            indexCursor = newCursor ? index.newFwdCursor(keyCursor.next()) : index.fwdCursor(keyCursor.next());

            if (indexCursor.hasNext()) {
                if (full) {
                    this.rowid = indexCursor.next();
                    return true;
                }

                do {
                    long rowid = indexCursor.next();
                    if (rowid > lo && rowid < hi) {
                        this.rowid = rowid;
                        return true;
                    }
                } while (indexCursor.hasNext());
            }
        }

        return false;
    }
}
