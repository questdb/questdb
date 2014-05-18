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

import com.nfsdb.journal.JournalBulkWriter;
import com.nfsdb.journal.JournalKey;
import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.PartitionType;
import com.nfsdb.journal.exceptions.JournalConfigurationException;
import com.nfsdb.journal.exceptions.JournalException;

import java.io.Closeable;
import java.io.File;

public class JournalFactory extends AbstractJournalReaderFactory implements JournalReaderFactory, JournalWriterFactory, Closeable {

    public JournalFactory(String journalBase) throws JournalConfigurationException {
        this(new File(journalBase));
    }

    public JournalFactory(File journalBase) throws JournalConfigurationException {
        this(new JournalConfiguration(journalBase).build());
    }

    public JournalFactory(JournalConfiguration configuration) {
        super(configuration);
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

    public <T> JournalWriter<T> writer(JournalKey<T> key) throws JournalException {
        return new JournalWriter<>(getConfiguration().getMetadata(key), key, getTimerCache());
    }

    public <T> JournalBulkWriter<T> bulkWriter(JournalKey<T> key) throws JournalException {
        return new JournalBulkWriter<>(getConfiguration().getMetadata(key), key, getTimerCache());
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
