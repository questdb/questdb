/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.test.tools;

import com.questdb.store.Files;
import com.questdb.store.factory.Factory;
import com.questdb.store.factory.configuration.JournalConfiguration;
import com.questdb.store.factory.configuration.JournalConfigurationBuilder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;

public class FactoryContainer implements TestRule {

    private final JournalConfigurationBuilder builder;
    private JournalConfiguration configuration;
    private Factory factory;

    public FactoryContainer(JournalConfigurationBuilder builder) {
        this.builder = builder;
    }

    public FactoryContainer() {
        this.builder = new JournalConfigurationBuilder();
    }

    @Override
    public Statement apply(final Statement base, final Description desc) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                Throwable throwable = null;
                File tmp = Files.makeTempDir();

                factory = null;

                try {
                    configuration = builder.build(tmp);
                    base.evaluate();
                } catch (Throwable e) {
                    throwable = e;
                } finally {

                    if (factory != null) {
                        factory.close();
                    }

                    com.questdb.store.Files.deleteOrException(tmp);
                }

                if (throwable != null) {
                    throw throwable;
                }
            }
        };
    }

    public JournalConfiguration getConfiguration() {
        return configuration;
    }

    public Factory getFactory() {
        if (factory == null) {
            factory = new Factory(configuration);
        }
        return factory;
    }
}