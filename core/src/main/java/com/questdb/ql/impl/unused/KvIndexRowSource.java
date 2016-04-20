/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 ******************************************************************************/

package com.questdb.ql.impl.unused;

import com.questdb.ex.JournalException;
import com.questdb.ex.JournalRuntimeException;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.ql.KeyCursor;
import com.questdb.ql.KeySource;
import com.questdb.ql.PartitionSlice;
import com.questdb.ql.RowCursor;
import com.questdb.ql.impl.AbstractRowSource;
import com.questdb.store.IndexCursor;
import com.questdb.store.KVIndex;
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
