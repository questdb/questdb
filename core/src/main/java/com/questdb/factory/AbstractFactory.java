/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

abstract class AbstractFactory implements Closeable {
    private final JournalConfiguration configuration;
    private final long inactiveTtlMs;
    protected FactoryEventListener eventListener;

    public AbstractFactory(JournalConfiguration configuration, long inactiveTtlMs) {
        this.configuration = configuration;
        this.inactiveTtlMs = inactiveTtlMs;
    }

    public AbstractFactory(String databaseHome, long inactiveTtlMs) {
        this(new JournalConfigurationBuilder().build(databaseHome), inactiveTtlMs);
    }

    @Override
    public void close() {
    }

    public JournalConfiguration getConfiguration() {
        return configuration;
    }

    public FactoryEventListener getEventListener() {
        return eventListener;
    }

    public void setEventListener(FactoryEventListener eventListener) {
        this.eventListener = eventListener;
    }

    public boolean releaseInactive() {
        return releaseAll(System.currentTimeMillis() - inactiveTtlMs);
    }

    protected abstract boolean releaseAll(long deadline);
}
