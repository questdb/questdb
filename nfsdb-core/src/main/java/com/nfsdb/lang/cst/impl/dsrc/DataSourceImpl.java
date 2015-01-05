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

package com.nfsdb.lang.cst.impl.dsrc;

import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.lang.cst.DataSource;
import com.nfsdb.lang.cst.impl.qry.JournalRecord;
import com.nfsdb.lang.cst.impl.qry.JournalRecordSource;

public class DataSourceImpl<T> extends AbstractImmutableIterator<T> implements DataSource<T> {
    private final JournalRecordSource journalRowSource;
    private final T container;

    public DataSourceImpl(JournalRecordSource journalRowSource, T container) {
        this.journalRowSource = journalRowSource;
        this.container = container;
    }

    @Override
    public boolean hasNext() {
        return journalRowSource.hasNext();
    }

    @SuppressWarnings("unchecked")
    @Override
    public T next() {
        JournalRecord item = journalRowSource.next();
        item.partition.read(item.rowid, container);
        return container;
    }

    @Override
    public DataSource<T> $new() {
        journalRowSource.reset();
        return this;
    }

    @Override
    public T head() {
        if (hasNext()) {
            return next();
        } else {
            return null;
        }
    }
}
