/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 ******************************************************************************/
package com.nfsdb.ql.impl;

import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.ql.PartitionSlice;
import com.nfsdb.ql.RowCursor;
import com.nfsdb.ql.RowSource;
import com.nfsdb.ql.SymFacade;

public class TopRowSource extends AbstractRowSource {

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
    public void configure(JournalMetadata metadata) {
        delegate.configure(metadata);
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

    @Override
    public void prepare(SymFacade facade) {
        delegate.prepare(facade);
    }

    @Override
    public RowCursor prepareCursor(PartitionSlice partition) {
        this.cursor = delegate.prepareCursor(partition);
        return this;
    }

    @Override
    public void reset() {
        delegate.reset();
        this.remaining = count;

    }
}
