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

package com.nfsdb.lang.cst.impl.jsrc;

import com.nfsdb.Journal;
import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.lang.cst.StatefulJournalSource;
import com.nfsdb.lang.cst.impl.qry.JournalRecord;
import com.nfsdb.lang.cst.impl.qry.JournalRecordSource;
import com.nfsdb.lang.cst.impl.qry.RecordMetadata;

public class StatefulJournalSourceImpl extends AbstractImmutableIterator<JournalRecord> implements StatefulJournalSource {
    private final JournalRecordSource delegate;
    private JournalRecord current;

    public StatefulJournalSourceImpl(JournalRecordSource delegate) {
        this.delegate = delegate;
    }

    @Override
    public JournalRecord current() {
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
    public JournalRecord next() {
        return current = delegate.next();
    }

    @Override
    public RecordMetadata getMetadata() {
        return delegate.getMetadata();
    }
}
