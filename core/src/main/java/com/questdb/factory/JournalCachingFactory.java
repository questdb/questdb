/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (c) 2014-2016 Appsicle
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
 ******************************************************************************/

package com.questdb.factory;

import com.questdb.Journal;
import com.questdb.JournalBulkReader;
import com.questdb.JournalKey;
import com.questdb.ex.JournalException;
import com.questdb.factory.configuration.JournalConfiguration;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.std.ObjObjHashMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
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
