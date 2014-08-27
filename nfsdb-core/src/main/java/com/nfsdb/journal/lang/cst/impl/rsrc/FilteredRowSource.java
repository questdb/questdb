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

import com.nfsdb.journal.lang.cst.*;

public class FilteredRowSource implements RowSource, RowCursor {

    private final RowSource delegate;
    private final RowFilter filter;
    private RowCursor underlying;
    private RowAcceptor acceptor;
    private long rowid;
    private boolean skip;

    public FilteredRowSource(RowSource delegate, RowFilter filter) {
        this.delegate = delegate;
        this.filter = filter;
    }

    @Override
    public RowCursor cursor(PartitionSlice slice) {
        this.underlying = delegate.cursor(slice);
        this.acceptor = filter.acceptor(slice, null);
        this.rowid = -1;
        this.skip = false;
        return this;
    }

    @Override
    public boolean hasNext() {
        if (this.rowid == -1) {

            if (skip) {
                return false;
            }

            long rowid;

            A:
            while (underlying.hasNext()) {
                rowid = underlying.next();

                Choice choice = acceptor.accept(rowid, -1);
                switch (choice) {
                    case SKIP:
                        break;
                    case PICK:
                        this.rowid = rowid;
                        break A;
                    case PICK_AND_SKIP_PARTITION:
                        this.rowid = rowid;
                        this.skip = true;
                        break A;

                }
            }
        }
        return this.rowid > -1;
    }

    @Override
    public long next() {
        long rowid = this.rowid;
        this.rowid = -1;
        return rowid;
    }

    @Override
    public void reset() {
        delegate.reset();
    }
}
