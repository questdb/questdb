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

import com.questdb.*;
import com.questdb.ex.JournalException;
import com.questdb.factory.configuration.JournalConfiguration;
import com.questdb.factory.configuration.JournalConfigurationBuilder;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.factory.configuration.MetadataBuilder;

public class JournalFactory extends AbstractJournalReaderFactory implements JournalReaderFactory, JournalWriterFactory {

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
        return bulkWriter(new JournalKey<>(clazz, location, PartitionBy.DEFAULT, recordHint));
    }

    @Override
    public <T> JournalBulkWriter<T> bulkWriter(JournalKey<T> key) throws JournalException {
        return new JournalBulkWriter<>(getConfiguration().createMetadata(key), key);
    }

    @Override
    public <T> JournalWriter<T> bulkWriter(MetadataBuilder<T> b) throws JournalException {
        return bulkWriter(getConfiguration().buildWithRootLocation(b));
    }

    @Override
    public <T> JournalWriter<T> bulkWriter(JournalMetadata<T> metadata) throws JournalException {
        return new JournalBulkWriter<>(metadata, metadata.getKey());
    }

    @Override
    @SuppressWarnings("unchecked")
    public JournalWriter bulkWriter(String location) throws JournalException {
        return bulkWriter(new JournalKey<>(location));
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
        return writer(new JournalKey<>(clazz, location, PartitionBy.DEFAULT, recordHint));
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
