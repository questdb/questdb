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

package com.nfsdb.journal.lang.cst.impl.jsrc;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.collections.AbstractImmutableIterator;
import com.nfsdb.journal.lang.cst.DataItem;
import com.nfsdb.journal.lang.cst.JournalSource;
import com.nfsdb.journal.lang.cst.StatefulJournalSource;

public class StatefulJournalSourceImpl extends AbstractImmutableIterator<DataItem> implements StatefulJournalSource {
    private final JournalSource delegate;
    private DataItem current;

    public StatefulJournalSourceImpl(JournalSource delegate) {
        this.delegate = delegate;
    }

    @Override
    public DataItem current() {
        return current;
    }

    @Override
    public void reset() {
        delegate.reset();
    }

    @Override
    public Journal getJournal() {
        return delegate.getJournal();
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public DataItem next() {
        return current = delegate.next();
    }
}
