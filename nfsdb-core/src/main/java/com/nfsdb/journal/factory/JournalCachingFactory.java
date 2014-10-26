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

package com.nfsdb.journal.factory;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.JournalKey;
import com.nfsdb.journal.concurrent.TimerCache;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.factory.configuration.JournalConfiguration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JournalCachingFactory extends AbstractJournalReaderFactory implements JournalClosingListener {

    private final Map<JournalKey, Journal> journals = new HashMap<>();
    private final List<Journal> journalList = new ArrayList<>();

    private JournalPool pool;

    public JournalCachingFactory(JournalConfiguration configuration) {
        this(configuration, new TimerCache().start());
    }

    private JournalCachingFactory(JournalConfiguration configuration, TimerCache timerCache) {
        this(configuration, timerCache, null);
    }

    public JournalCachingFactory(JournalConfiguration configuration, TimerCache timerCache, JournalPool pool) {
        super(configuration, timerCache);
        this.pool = pool;
    }

    @Override
    public void close() {
        if (pool != null) {
            pool.release(this);
        } else {
            for (int i = 0, sz = journalList.size(); i < sz; i++) {
                Journal journal = journalList.get(i);
                journal.setCloseListener(null);
                journal.close();
            }
            journals.clear();
        }
    }

    @Override
    public boolean closing(Journal journal) {
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Journal<T> reader(JournalKey<T> key) throws JournalException {
        Journal<T> result = journals.get(key);
        if (result == null) {
            result = super.reader(key);
            result.setCloseListener(this);
            journals.put(key, result);
            journalList.add(result);
        }
        return result;
    }

    public void refresh() throws JournalException {
        for (int i = 0, sz = journalList.size(); i < sz; i++) {
            journalList.get(i).refresh();
        }
    }

    void clearPool() {
        this.pool = null;
    }

    void expireOpenFiles() {
        for (int i = 0, sz = journalList.size(); i < sz; i++) {
            journalList.get(i).expireOpenFiles();
        }
    }
}
