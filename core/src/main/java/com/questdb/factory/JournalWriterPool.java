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
import com.questdb.JournalWriter;
import com.questdb.ex.JournalException;
import com.questdb.factory.configuration.JournalConfiguration;
import com.questdb.factory.configuration.JournalMetadata;

import java.util.concurrent.ConcurrentHashMap;

public class JournalWriterPool extends WriterFactoryImpl implements JournalCloseInterceptor {

    public static final int ALLOC_BEGIN = 1;
    public static final int ALLOC_FAILURE = 2;

    private final ConcurrentHashMap<String, Integer> allocMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, JournalWriter> writerMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> busyMap = new ConcurrentHashMap<>();

    public JournalWriterPool(String databaseHome) {
        super(databaseHome);
    }

    public JournalWriterPool(JournalConfiguration configuration) {
        super(configuration);
    }

    @Override
    public <T> JournalWriter<T> bulkWriter(JournalMetadata<T> metadata) throws JournalException {
        return get(metadata, true);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> JournalWriter<T> writer(JournalMetadata<T> metadata) throws JournalException {
        return get(metadata, false);
    }

    @Override
    public boolean canClose(Journal journal) {
        String path = journal.getName();
        if (writerMap.get(path) == journal && busyMap.get(path) == Thread.currentThread().getId()) {
            busyMap.put(path, -1L);
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private <T> JournalWriter<T> get(JournalMetadata<T> metadata, boolean bulk) throws JournalException {
        final String path = metadata.getKey().path();

        if (allocMap.putIfAbsent(path, ALLOC_BEGIN) == null) {
            try {
                JournalWriter w = bulk ? super.bulkWriter(metadata) : super.writer(metadata);
                w.setCloseInterceptor(this);
                writerMap.put(path, w);
                busyMap.put(path, Thread.currentThread().getId());
                return w;
            } catch (JournalException e) {
                allocMap.put(path, ALLOC_FAILURE);
                throw e;
            }
        }


        if (allocMap.get(path) == ALLOC_BEGIN && busyMap.replace(path, -1L, Thread.currentThread().getId())) {
            return writerMap.get(path);
        }

        return null;
    }
}
