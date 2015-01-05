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
import com.nfsdb.journal.lang.cst.impl.qry.JournalRecord;
import com.nfsdb.journal.lang.cst.impl.qry.JournalRecordSource;
import com.nfsdb.journal.lang.cst.impl.qry.RecordMetadata;

public class TopJournalSource extends AbstractImmutableIterator<JournalRecord> implements JournalRecordSource {

    private final JournalRecordSource delegate;
    private final int count;
    private int remaining;

    public TopJournalSource(int count, JournalRecordSource delegate) {
        this.delegate = delegate;
        this.count = count;
        this.remaining = count;
    }

    @Override
    public void reset() {
        delegate.reset();
        this.remaining = count;
    }

    @Override
    public boolean hasNext() {
        return remaining > 0 && delegate.hasNext();
    }

    @Override
    public JournalRecord next() {
        remaining--;
        return delegate.next();
    }

    @Override
    public Journal getJournal() {
        return delegate.getJournal();
    }

    @Override
    public RecordMetadata getMetadata() {
        return delegate.getMetadata();
    }
}
