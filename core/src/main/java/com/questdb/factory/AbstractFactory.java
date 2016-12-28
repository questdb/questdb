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

import com.questdb.factory.configuration.JournalConfiguration;
import com.questdb.factory.configuration.JournalConfigurationBuilder;

import java.io.Closeable;

public class AbstractFactory implements Closeable {
    private final JournalConfiguration configuration;

    public AbstractFactory(JournalConfiguration configuration) {
        this.configuration = configuration;
    }

    public AbstractFactory(String databaseHome) {
        this(new JournalConfigurationBuilder().build(databaseHome));
    }

    @Override
    public void close() {
    }

    public JournalConfiguration getConfiguration() {
        return configuration;
    }
}
