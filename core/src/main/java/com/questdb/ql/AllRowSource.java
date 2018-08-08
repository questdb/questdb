/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.ql;

import com.questdb.std.ex.JournalException;
import com.questdb.std.str.CharSink;
import com.questdb.store.JournalRuntimeException;
import com.questdb.store.RowCursor;
import com.questdb.store.StorageFacade;
import com.questdb.store.factory.ReaderFactory;
import com.questdb.store.factory.configuration.JournalMetadata;

public class AllRowSource implements RowSource, RowCursor {
    private long lo;
    private long hi;

    @Override
    public void configure(JournalMetadata metadata) {
    }

    @Override
    public void prepare(ReaderFactory factory, StorageFacade storageFacade, CancellationHandler cancellationHandler) {

    }

    @Override
    public RowCursor prepareCursor(PartitionSlice slice) {
        try {
            this.lo = slice.lo;
            this.hi = slice.calcHi ? slice.partition.open().size() - 1 : slice.hi;
            return this;
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
    }

    @Override
    public void toTop() {

    }

    @Override
    public boolean hasNext() {
        return lo <= hi;
    }

    @Override
    public long next() {
        return lo++;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("AllRowSource");
        sink.put('}');
    }
}
