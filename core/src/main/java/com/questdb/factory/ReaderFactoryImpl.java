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
import com.questdb.JournalKey;
import com.questdb.PartitionBy;
import com.questdb.ex.JournalException;
import com.questdb.factory.configuration.JournalConfiguration;
import com.questdb.factory.configuration.JournalMetadata;

public class ReaderFactoryImpl extends AbstractFactory implements ReaderFactory {

    public ReaderFactoryImpl(String databaseHome) {
        super(databaseHome);
    }

    public ReaderFactoryImpl(JournalConfiguration configuration) {
        super(configuration);
    }

    public <T> JournalMetadata<T> getOrCreateMetadata(JournalKey<T> key) throws JournalException {
        return getConfiguration().createMetadata(key);
    }

    @Override
    public final <T> Journal<T> reader(JournalKey<T> key) throws JournalException {
        return reader(getOrCreateMetadata(key));
    }

    @Override
    public final <T> Journal<T> reader(Class<T> clazz) throws JournalException {
        return reader(new JournalKey<>(clazz));
    }

    @Override
    public final <T> Journal<T> reader(Class<T> clazz, String name) throws JournalException {
        return reader(new JournalKey<>(clazz, name));
    }

    @Override
    public final Journal reader(String name) throws JournalException {
        return reader(new JournalKey<>(name));
    }

    @Override
    public final <T> Journal<T> reader(Class<T> clazz, String location, int recordHint) throws JournalException {
        return reader(new JournalKey<>(clazz, location, PartitionBy.DEFAULT, recordHint));
    }

    @Override
    public <T> Journal<T> reader(JournalMetadata<T> metadata) throws JournalException {
        return new Journal<>(metadata);
    }
}
