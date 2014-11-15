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

package com.nfsdb.journal.lang.cst.impl.join;

import com.nfsdb.journal.collections.AbstractImmutableIterator;
import com.nfsdb.journal.lang.cst.EntrySource;
import com.nfsdb.journal.lang.cst.JournalEntry;

import java.util.NoSuchElementException;

public class InnerSkipJoin extends AbstractImmutableIterator<JournalEntry> implements EntrySource {

    private final EntrySource delegate;
    private JournalEntry data;

    public InnerSkipJoin(EntrySource delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
        JournalEntry data;

        while (delegate.hasNext()) {
            if ((data = delegate.next()).slave != null) {
                this.data = data;
                return true;
            }
        }
        this.data = null;
        return false;
    }

    @Override
    public JournalEntry next() {
        if (data == null) {
            throw new NoSuchElementException();
        }
        return data;
    }

    @Override
    public void reset() {
        delegate.reset();
    }
}
