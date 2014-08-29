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

import com.nfsdb.journal.lang.cst.PartitionSlice;
import com.nfsdb.journal.lang.cst.RowCursor;
import com.nfsdb.journal.lang.cst.RowSource;

public class TopRowSource implements RowSource, RowCursor {

    private final RowSource delegate;
    private final int count;
    private int remaining;
    private RowCursor cursor;


    public TopRowSource(int count, RowSource delegate) {
        this.delegate = delegate;
        this.count = count;
        this.remaining = count;
    }

    @Override
    public RowCursor cursor(PartitionSlice partition) {
        this.cursor = delegate.cursor(partition);
        return this;
    }

    @Override
    public void reset() {
        delegate.reset();
        this.remaining = count;

    }

    @Override
    public boolean hasNext() {
        return remaining > 0 && cursor.hasNext();
    }

    @Override
    public long next() {
        remaining--;
        return cursor.next();
    }
}
