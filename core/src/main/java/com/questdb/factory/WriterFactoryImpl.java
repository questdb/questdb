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

import com.questdb.JournalKey;
import com.questdb.JournalWriter;
import com.questdb.PartitionBy;
import com.questdb.ex.JournalException;
import com.questdb.factory.configuration.JournalConfiguration;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.factory.configuration.MetadataBuilder;

public class WriterFactoryImpl extends AbstractFactory implements WriterFactory {

    public WriterFactoryImpl(String databaseHome) {
        super(databaseHome);
    }

    public WriterFactoryImpl(JournalConfiguration configuration) {
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
        return writer(new JournalKey<>(clazz, location, PartitionBy.DEFAULT, recordHint));
    }

    @Override
    public JournalWriter writer(String location) throws JournalException {
        return writer(new JournalKey<>(location));
    }

    @Override
    public <T> JournalWriter<T> writer(JournalKey<T> key) throws JournalException {
        return writer(getConfiguration().createMetadata(key));
    }

    @Override
    public <T> JournalWriter<T> writer(MetadataBuilder<T> metadataBuilder) throws JournalException {
        return writer(getConfiguration().buildWithRootLocation(metadataBuilder));
    }

    @Override
    public <T> JournalWriter<T> writer(JournalMetadata<T> metadata) throws JournalException {
        return new JournalWriter<>(metadata);
    }
}
