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
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalImmutableIteratorException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;

import java.util.Iterator;

public class ResultSetBufferedIterator<T> implements JournalIterator<T> {

    private final ResultSet<T> rs;
    private final T obj;
    private int counter = 0;

    public ResultSetBufferedIterator(ResultSet<T> rs) {
        this.rs = rs;
        this.obj = rs.getJournal().newObject();
    }

    @Override
    public boolean hasNext() {
        return counter < rs.size();
    }

    @Override
    public T next() {
        try {
            rs.getJournal().clearObject(obj);
            rs.read(counter++, obj);
            return obj;
        } catch (JournalException e) {
            throw new JournalRuntimeException("Journal exception", e);
        }
    }

    @Override
    public void remove() {
        throw new JournalImmutableIteratorException();
    }

    @Override
    public Iterator<T> iterator() {
        return this;
    }

    @Override
    public Journal<T> getJournal() {
        return rs.getJournal();
    }
}
