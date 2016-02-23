/*******************************************************************************
 * _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 * <p/>
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.factory;

import com.nfsdb.Journal;
import com.nfsdb.JournalBulkReader;
import com.nfsdb.JournalKey;
import com.nfsdb.ex.JournalException;
import com.nfsdb.factory.configuration.JournalConfiguration;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.std.ObjObjHashMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
public class JournalCachingFactory extends AbstractJournalReaderFactory implements JournalClosingListener {
    private final ObjObjHashMap<JournalKey, Journal> readers = new ObjObjHashMap<>();
    private final ObjObjHashMap<JournalKey, JournalBulkReader> bulkReaders = new ObjObjHashMap<>();
    private final List<Journal> journalList = new ArrayList<>();
    private JournalFactoryPool pool;
    private boolean inPool = false;
    private AtomicBoolean closed = new AtomicBoolean(false);

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

    void setInUse() {
        inPool = false;
    }
}
