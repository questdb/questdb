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

package com.nfsdb.journal.map;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.utils.Unsafe;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class JournalHashMap<T> implements JournalMap<T> {
    private final HashMap<String, T> map;
    private final Set<String> invalidKeyCache;
    private final Journal<T> journal;
    private final String column;
    private final long columnOffset;
    private final JournalMapFilter<T> filter;
    private boolean eager = false;

    public JournalHashMap(Journal<T> journal) {
        this(journal, journal.getMetadata().getKey());
    }

    public JournalHashMap(Journal<T> journal, String column) {
        this(journal, column, null);
    }

    public JournalHashMap(Journal<T> journal, String column, JournalMapFilter<T> filter) {
        this.journal = journal;
        this.column = column;
        this.filter = filter;
        this.map = new HashMap<>(journal.getSymbolTable(column).size());
        this.columnOffset = journal.getMetadata().getColumnMetadata(column).offset;
        this.invalidKeyCache = new HashSet<>();
    }

    @Override
    public JournalMap<T> eager() throws JournalException {
        for (T t : journal.query().head().withSymValues(column).asResultSet()) {
            if (filter == null || filter.accept(t)) {
                map.put((String) Unsafe.getUnsafe().getObject(t, columnOffset), t);
            }
        }
        eager = true;
        return this;
    }

    @Override
    public T get(String key) {
        T result = map.get(key);
        if (result == null && !eager) {
            if (!invalidKeyCache.contains(key)) {
                try {
                    result = journal.query().head().withSymValues(column, key).asResultSet().readFirst();
                } catch (JournalException e) {
                    throw new JournalRuntimeException(e);
                }

                if (filter != null && result != null && !filter.accept(result)) {
                    result = null;
                }

                if (result != null) {
                    map.put((String) Unsafe.getUnsafe().getObject(result, columnOffset), result);
                } else {
                    invalidKeyCache.add(key);
                }
            }
        }
        return result;
    }

    @Override
    public Set<String> keys() {
        return map.keySet();
    }

    @Override
    public String getColumn() {
        return column;
    }

    @Override
    public Collection<T> values() {
        return map.values();
    }

    @Override
    public boolean refresh() throws JournalException {
        if (journal.refresh()) {
            map.clear();
            if (eager) {
                eager();
            }
            return true;
        }
        return false;
    }

    @Override
    public int size() {
        return map.size();
    }
}
