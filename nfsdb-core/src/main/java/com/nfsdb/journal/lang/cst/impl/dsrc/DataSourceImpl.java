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

package com.nfsdb.journal.lang.cst.impl.dsrc;

import com.nfsdb.journal.collections.AbstractImmutableIterator;
import com.nfsdb.journal.lang.cst.DataSource;
import com.nfsdb.journal.lang.cst.JournalEntry;
import com.nfsdb.journal.lang.cst.JournalSource;

public class DataSourceImpl<T> extends AbstractImmutableIterator<T> implements DataSource<T> {
    private final JournalSource journalSource;
    private final T container;

    public DataSourceImpl(JournalSource journalSource, T container) {
        this.journalSource = journalSource;
        this.container = container;
    }

    @Override
    public boolean hasNext() {
        return journalSource.hasNext();
    }

    @SuppressWarnings("unchecked")
    @Override
    public T next() {
        JournalEntry item = journalSource.next();
        item.partition.read(item.rowid, container);
        return container;
    }

    @Override
    public DataSource<T> $new() {
        journalSource.reset();
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
