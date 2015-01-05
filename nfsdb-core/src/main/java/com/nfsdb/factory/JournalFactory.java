/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

import com.nfsdb.JournalBulkWriter;
import com.nfsdb.JournalKey;
import com.nfsdb.JournalWriter;
import com.nfsdb.PartitionType;
import com.nfsdb.concurrent.TimerCache;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.configuration.JMetadataBuilder;
import com.nfsdb.factory.configuration.JournalConfiguration;
import com.nfsdb.factory.configuration.JournalConfigurationBuilder;
import com.nfsdb.factory.configuration.JournalMetadata;

import java.io.Closeable;
import java.io.File;

public class JournalFactory extends AbstractJournalReaderFactory implements JournalReaderFactory, JournalWriterFactory, Closeable {

    public JournalFactory(String journalBase) {
        super(new JournalConfigurationBuilder().build(journalBase));
    }

    public JournalFactory(File journalBase) {
        super(new JournalConfigurationBuilder().build(journalBase));
    }

    public JournalFactory(JournalConfiguration configuration) {
        super(configuration);
    }

    protected JournalFactory(JournalConfiguration configuration, TimerCache timerCache) {
        super(configuration, timerCache);
    }

    @Override
    public <T> JournalWriter<T> writer(Class<T> clazz) throws JournalException {
        return writer(new JournalKey<>(clazz));
    }

    @Override
    public <T> JournalWriter<T> writer(Class<T> clazz, String location) throws JournalException {
        return writer(new JournalKey<>(clazz, location));
    }

    @Override
    public <T> JournalWriter<T> writer(Class<T> clazz, String location, int recordHint) throws JournalException {
        return writer(new JournalKey<>(clazz, location, PartitionType.DEFAULT, recordHint));
    }

    @Override
    public JournalWriter writer(String location) throws JournalException {
        return writer(new JournalKey<>(location));
    }

    @Override
    public <T> JournalWriter<T> writer(JournalKey<T> key) throws JournalException {
        return new JournalWriter<>(getConfiguration().createMetadata(key), key, getTimerCache());
    }

    @Override
    public <T> JournalWriter<T> writer(JMetadataBuilder<T> b) throws JournalException {
        JournalMetadata<T> metadata = getConfiguration().augmentMetadata(b);
        return new JournalWriter<>(metadata, metadata.deriveKey(), getTimerCache());
    }

    @Override
    public <T> JournalBulkWriter<T> bulkWriter(JournalKey<T> key) throws JournalException {
        return new JournalBulkWriter<>(getConfiguration().createMetadata(key), key, getTimerCache());
    }

    @Override
    public <T> JournalBulkWriter<T> bulkWriter(Class<T> clazz) throws JournalException {
        return bulkWriter(new JournalKey<>(clazz));
    }

    @Override
    public <T> JournalBulkWriter<T> bulkWriter(Class<T> clazz, String location) throws JournalException {
        return bulkWriter(new JournalKey<>(clazz, location));
    }

    @Override
    public <T> JournalBulkWriter<T> bulkWriter(Class<T> clazz, String location, int recordHint) throws JournalException {
        return bulkWriter(new JournalKey<>(clazz, location, PartitionType.DEFAULT, recordHint));
    }
}
