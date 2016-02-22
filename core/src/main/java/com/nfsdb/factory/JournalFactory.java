/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

package com.nfsdb.factory;

import com.nfsdb.*;
import com.nfsdb.ex.JournalException;
import com.nfsdb.factory.configuration.JournalConfiguration;
import com.nfsdb.factory.configuration.JournalConfigurationBuilder;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.factory.configuration.MetadataBuilder;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class JournalFactory extends AbstractJournalReaderFactory implements JournalReaderFactory, JournalWriterFactory {

    @SuppressFBWarnings({"SCII_SPOILED_CHILD_INTERFACE_IMPLEMENTOR"})
    public JournalFactory(String journalBase) {
        super(new JournalConfigurationBuilder().build(journalBase));
    }

    public JournalFactory(JournalConfiguration configuration) {
        super(configuration);
    }

    @Override
    public <T> JournalBulkReader<T> bulkReader(JournalKey<T> key) throws JournalException {
        return new JournalBulkReader<>(getOrCreateMetadata(key), key);
    }

    @Override
    public <T> Journal<T> reader(JournalKey<T> key) throws JournalException {
        return new Journal<>(getOrCreateMetadata(key), key);
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

    @Override
    public <T> JournalBulkWriter<T> bulkWriter(JournalKey<T> key) throws JournalException {
        return new JournalBulkWriter<>(getConfiguration().createMetadata(key), key);
    }

    @Override
    public <T> JournalWriter<T> bulkWriter(MetadataBuilder<T> b) throws JournalException {
        JournalMetadata<T> metadata = getConfiguration().buildWithRootLocation(b);
        return new JournalBulkWriter<>(metadata, metadata.getKey());
    }

    @Override
    public <T> JournalWriter<T> bulkWriter(JournalMetadata<T> metadata) throws JournalException {
        return new JournalBulkWriter<>(metadata, metadata.getKey());
    }

    @Override
    public JournalWriter bulkWriter(String location) throws JournalException {
        return bulkWriter(new JournalKey(location));
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
        return new JournalWriter<>(getConfiguration().createMetadata(key), key);
    }

    @Override
    public <T> JournalWriter<T> writer(MetadataBuilder<T> b) throws JournalException {
        JournalMetadata<T> metadata = getConfiguration().buildWithRootLocation(b);
        return new JournalWriter<>(metadata, metadata.getKey());
    }

    /**
     * Inherited method, this implementation does not own created journals
     * and has nothing to close.
     */
    @Override
    public void close() {
    }
}
