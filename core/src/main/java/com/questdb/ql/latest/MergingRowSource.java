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

package com.questdb.ql.latest;

import com.questdb.ql.CancellationHandler;
import com.questdb.ql.PartitionSlice;
import com.questdb.ql.RowSource;
import com.questdb.std.str.CharSink;
import com.questdb.store.RowCursor;
import com.questdb.store.StorageFacade;
import com.questdb.store.factory.ReaderFactory;
import com.questdb.store.factory.configuration.JournalMetadata;

public class MergingRowSource implements RowSource, RowCursor {
    private final RowSource lhs;
    private final RowSource rhs;
    private RowCursor lhc;
    private RowCursor rhc;
    private long nxtl;
    private long nxtr;

    public MergingRowSource(RowSource lhs, RowSource rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    @Override
    public void configure(JournalMetadata metadata) {
        this.lhs.configure(metadata);
        this.rhs.configure(metadata);
    }

    @Override
    public void prepare(ReaderFactory factory, StorageFacade facade, CancellationHandler cancellationHandler) {
        lhs.prepare(factory, facade, cancellationHandler);
        rhs.prepare(factory, facade, cancellationHandler);
    }

    @Override
    public RowCursor prepareCursor(PartitionSlice slice) {
        this.lhc = lhs.prepareCursor(slice);
        this.rhc = rhs.prepareCursor(slice);
        nxtl = -1;
        nxtr = -1;
        return this;
    }

    @Override
    public void toTop() {
        this.lhs.toTop();
        this.rhs.toTop();
        this.nxtl = -1;
        this.nxtr = -1;
    }

    @Override
    public boolean hasNext() {
        return nxtl > -1 || lhc.hasNext() || nxtr > -1 || rhc.hasNext();
    }

    @Override
    public long next() {
        long result;

        if (nxtl == -1 && lhc.hasNext()) {
            nxtl = lhc.next();
        }

        if (nxtr == -1 && rhc.hasNext()) {
            nxtr = rhc.next();
        }

        if (nxtr == -1 || (nxtl > -1 && nxtl < nxtr)) {
            result = nxtl;
            nxtl = -1;
        } else {
            result = nxtr;
            nxtr = -1;
        }

        return result;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("MergingRowSource").put(',');
        sink.putQuoted("left").put(':').put(lhs).put(',');
        sink.putQuoted("right").put(':').put(rhs);
        sink.put('}');
    }
}
