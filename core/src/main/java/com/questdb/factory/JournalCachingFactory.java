/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.factory;

import com.questdb.Journal;
import com.questdb.JournalBulkReader;
import com.questdb.JournalKey;
import com.questdb.ex.JournalException;
import com.questdb.factory.configuration.JournalConfiguration;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.std.ObjObjHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class JournalCachingFactory extends AbstractJournalReaderFactory implements JournalClosingListener {
    private final ObjObjHashMap<JournalKey, Journal> readers = new ObjObjHashMap<>();
    private final ObjObjHashMap<JournalKey, JournalBulkReader> bulkReaders = new ObjObjHashMap<>();
    private final List<Journal> journalList = new ArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private JournalFactoryPool pool;
    private boolean inPool = false;

    public JournalCachingFactory(JournalConfiguration configuration) {
        super(configuration);
    }

    public JournalCachingFactory(JournalConfiguration configuration, JournalFactoryPool pool) {
        super(configuration);
        this.pool = pool;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> JournalBulkReader<T> bulkReader(JournalKey<T> key) throws JournalException {
        JournalBulkReader<T> result = bulkReaders.get(key);
        if (result == null) {
            result = new JournalBulkReader<>(getOrCreateMetadata(key), key);
            result.setCloseListener(this);
            bulkReaders.put(key, result);
            journalList.add(result);
        }
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Journal<T> reader(JournalKey<T> key) throws JournalException {
        Journal<T> result = readers.get(key);
        if (result == null) {
            result = new Journal<>(getOrCreateMetadata(key), key);
            result.setCloseListener(this);
            readers.put(key, result);
            journalList.add(result);
        }
        return result;
    }

    @Override
    public void close() {
        if (pool != null) {
            // To not release twice.
            if (!inPool) {
                inPool = true;
                pool.release(this);
            }
        } else {
            if (closed.compareAndSet(false, true)) {
                for (int i = 0, sz = journalList.size(); i < sz; i++) {
                    Journal journal = journalList.get(i);
                    journal.setCloseListener(null);
                    journal.close();
                }
                readers.clear();
                bulkReaders.clear();
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Journal reader(JournalMetadata metadata) throws JournalException {
        JournalKey key = metadata.getKey();
        Journal result = readers.get(key);
        if (result == null) {
            result = new Journal<>(metadata, key);
            result.setCloseListener(this);
            readers.put(key, result);
            journalList.add(result);
        }
        return result;
    }

    @Override
    public boolean closing(Journal journal) {
        return false;
    }

    public void refresh() {
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

    void setInUse() {
        inPool = false;
    }
}
