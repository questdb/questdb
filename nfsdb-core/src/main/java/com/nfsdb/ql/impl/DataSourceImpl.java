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

package com.nfsdb.ql.impl;

import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.ql.DataSource;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.RecordSource;

public class DataSourceImpl<T> extends AbstractImmutableIterator<T> implements DataSource<T> {
    private final RecordSource<JournalRecord> recordSource;
    private final T container;
    private RecordCursor<JournalRecord> recordCursor;

    public DataSourceImpl(RecordSource<JournalRecord> recordSource, T container) {
        this.recordSource = recordSource;
        this.container = container;
        this.recordCursor = recordSource.prepareCursor();

    }

    @Override
    public DataSource<T> $new() {
        recordSource.unprepare();
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

    @Override
    public boolean hasNext() {
        return recordCursor.hasNext();
    }

    @SuppressWarnings("unchecked")
    @Override
    public T next() {
        JournalRecord item = recordCursor.next();
        item.partition.read(item.rowid, container);
        return container;
    }
}
