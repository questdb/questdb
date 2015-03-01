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

package com.nfsdb.query.iterator;

import com.nfsdb.Journal;
import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.query.ResultSet;

public class ResultSetIterator<T> extends AbstractImmutableIterator<T> implements JournalIterator<T>, PeekingIterator<T> {

    private final ResultSet<T> rs;
    private int cursor = 0;

    public ResultSetIterator(ResultSet<T> rs) {
        this.rs = rs;
    }

    @Override
    public Journal<T> getJournal() {
        return rs.getJournal();
    }

    @Override
    public boolean hasNext() {
        return cursor < rs.size();
    }

    @Override
    public T next() {
        try {
            return rs.read(cursor++);
        } catch (JournalException e) {
            throw new JournalRuntimeException("Journal exception", e);
        }
    }

    @Override
    public boolean isEmpty() {
        return cursor >= rs.size();
    }

    @Override
    public T peekFirst() {
        try {
            return rs.readFirst();
        } catch (JournalException e) {
            throw new JournalRuntimeException("Journal exception", e);
        }
    }

    @Override
    public T peekLast() {
        try {
            return rs.readLast();
        } catch (JournalException e) {
            throw new JournalRuntimeException("Journal exception", e);
        }
    }
}
