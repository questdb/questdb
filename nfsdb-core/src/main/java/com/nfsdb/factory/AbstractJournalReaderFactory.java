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

package com.nfsdb.factory;

import com.nfsdb.*;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.configuration.JournalConfiguration;
import com.nfsdb.factory.configuration.JournalMetadata;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Closeable;
import java.io.File;

@SuppressFBWarnings({"PATH_TRAVERSAL_IN"})
public abstract class AbstractJournalReaderFactory implements JournalReaderFactory, Closeable {

    private final TimerCache timerCache;
    private final JournalConfiguration configuration;
    private final boolean ownTimerCache;

    AbstractJournalReaderFactory(JournalConfiguration configuration) {
        this.configuration = configuration;
        this.timerCache = new TimerCache().start();
        this.ownTimerCache = true;
    }

    AbstractJournalReaderFactory(JournalConfiguration configuration, TimerCache timerCache) {
        this.timerCache = timerCache;
        this.configuration = configuration;
        this.ownTimerCache = false;
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
    public JournalBulkReader bulkReader(String location) throws JournalException {
        return bulkReader(new JournalKey<>(location));
    }

    @Override
    public void close() {
        if (ownTimerCache) {
            timerCache.halt();
        }
    }


    public JournalConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public <T> Journal<T> reader(Class<T> clazz) throws JournalException {
        return reader(new JournalKey<>(clazz));
    }

    @Override
    public <T> Journal<T> reader(Class<T> clazz, String location) throws JournalException {
        return reader(new JournalKey<>(clazz, location));
    }

    @Override
    public Journal reader(String location) throws JournalException {
        return reader(new JournalKey<>(location));
    }

    @Override
    public <T> Journal<T> reader(Class<T> clazz, String location, int recordHint) throws JournalException {
        return reader(new JournalKey<>(clazz, location, PartitionType.DEFAULT, recordHint));
    }

    <T> JournalMetadata<T> getOrCreateMetadata(JournalKey<T> key) throws JournalException {
        JournalMetadata<T> metadata = configuration.createMetadata(key);
        File location = new File(metadata.getLocation());
        if (!location.exists()) {
            new JournalWriter<>(metadata, key, timerCache).close();
        }
        return metadata;
    }

    protected TimerCache getTimerCache() {
        return timerCache;
    }

}
