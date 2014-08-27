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

package com.nfsdb.journal.iterators;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.ResultSet;
import com.nfsdb.journal.collections.AbstractImmutableIterator;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;

public class ResultSetBufferedIterator<T> extends AbstractImmutableIterator<T> implements JournalIterator<T>, PeekingIterator<T> {

    private final ResultSet<T> rs;
    private final T obj;
    private final Journal<T> journal;
    private int cursor = 0;

    public ResultSetBufferedIterator(ResultSet<T> rs) {
        this.rs = rs;
        this.obj = rs.getJournal().newObject();
        this.journal = rs.getJournal();
    }

    @Override
    public boolean hasNext() {
        return cursor < rs.size();
    }

    @Override
    public T next() {
        return get(cursor++);
    }

    @Override
    public Journal<T> getJournal() {
        return rs.getJournal();
    }

    @Override
    public T peekLast() {
        return get(rs.size() - 1);
    }

    @Override
    public T peekFirst() {
        return get(0);
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    private T get(int rsIndex) {
        try {
            journal.clearObject(obj);
            rs.read(rsIndex, obj);
            return obj;
        } catch (JournalException e) {
            throw new JournalRuntimeException("Journal exception at [" + rsIndex + "]", e);
        }
    }
}
