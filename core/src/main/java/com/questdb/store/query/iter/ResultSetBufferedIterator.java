/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.store.query.iter;

import com.questdb.std.ex.JournalException;
import com.questdb.store.Journal;
import com.questdb.store.JournalRuntimeException;
import com.questdb.store.query.ResultSet;

public class ResultSetBufferedIterator<T> implements JournalIterator<T>, PeekingIterator<T>, com.questdb.std.ImmutableIterator<T> {

    private final ResultSet<T> rs;
    private final T obj;
    private int cursor = 0;

    public ResultSetBufferedIterator(ResultSet<T> rs) {
        this.rs = rs;
        this.obj = rs.getJournal().newObject();
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
        return get(cursor++);
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public T peekFirst() {
        return get(0);
    }

    @Override
    public T peekLast() {
        return get(rs.size() - 1);
    }

    private T get(int rsIndex) {
        try {
            rs.read(rsIndex, obj);
            return obj;
        } catch (JournalException e) {
            throw new JournalRuntimeException("Journal exception at [" + rsIndex + ']', e);
        }
    }
}
