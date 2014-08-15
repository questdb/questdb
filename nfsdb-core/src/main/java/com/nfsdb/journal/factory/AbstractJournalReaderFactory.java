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
import com.nfsdb.journal.JournalBulkReader;
import com.nfsdb.journal.JournalKey;
import com.nfsdb.journal.concurrent.TimerCache;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.factory.configuration.JournalConfiguration;

import java.io.Closeable;

public abstract class AbstractJournalReaderFactory implements JournalReaderFactory, Closeable {

    private final TimerCache timerCache;
    private final JournalConfiguration configuration;

    @Override
    public <T> Journal<T> reader(Class<T> clazz, String location) throws JournalException {
        return reader(new JournalKey<>(clazz, location));
    }

    @Override
    public <T> Journal<T> reader(Class<T> clazz) throws JournalException {
        return reader(new JournalKey<>(clazz));
    }

    @Override
    public <T> Journal<T> reader(JournalKey<T> key) throws JournalException {
        return new Journal<>(configuration.createMetadata(key), key, timerCache);
    }

    @Override
    public <T> JournalBulkReader<T> bulkReader(Class<T> clazz, String location) throws JournalException {
        return bulkReader(new JournalKey<>(clazz, location));
    }

    @Override
    public <T> JournalBulkReader<T> bulkReader(Class<T> clazz) throws JournalException {
        return bulkReader(new JournalKey<>(clazz));
    }

    @Override
    public <T> JournalBulkReader<T> bulkReader(JournalKey<T> key) throws JournalException {
        return new JournalBulkReader<>(configuration.createMetadata(key), key, timerCache);
    }

    public JournalConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public void close() {
    }

    protected AbstractJournalReaderFactory(JournalConfiguration configuration) {
        this(configuration, new TimerCache().start());
    }

    protected AbstractJournalReaderFactory(JournalConfiguration configuration, TimerCache timerCache) {
        this.timerCache = timerCache;
        this.configuration = configuration;
    }

    protected TimerCache getTimerCache() {
        return timerCache;
    }

}
