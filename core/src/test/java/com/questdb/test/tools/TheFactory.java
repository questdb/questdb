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

package com.questdb.test.tools;

import com.questdb.factory.CachingReaderFactory;
import com.questdb.factory.ReaderFactory;
import com.questdb.factory.ReaderFactoryPool;
import com.questdb.factory.WriterFactory;
import com.questdb.factory.configuration.JournalConfiguration;
import com.questdb.factory.configuration.JournalConfigurationBuilder;
import com.questdb.misc.Files;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;

public class TheFactory implements TestRule {

    private final JournalConfigurationBuilder builder;
    private JournalConfiguration configuration;
    private WriterFactory writerFactory;
    private ReaderFactory readerFactory;
    private ReaderFactoryPool readerFactoryPool;
    private CachingReaderFactory cachingReaderFactory;

    public TheFactory(JournalConfigurationBuilder builder) {
        this.builder = builder;
    }

    public TheFactory() {
        this.builder = new JournalConfigurationBuilder();
    }

    @Override
    public Statement apply(final Statement base, final Description desc) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                Throwable throwable = null;
                File tmp = Files.makeTempDir();
                writerFactory = null;
                readerFactory = null;
                cachingReaderFactory = null;
                try {
                    configuration = builder.build(tmp);
                    base.evaluate();
                } catch (Throwable e) {
                    throwable = e;
                } finally {
                    if (cachingReaderFactory != null) {
                        cachingReaderFactory.close();
                    }

                    if (readerFactoryPool != null) {
                        readerFactoryPool.close();
                    }

                    Files.deleteOrException(tmp);
                }

                if (throwable != null) {
                    throw throwable;
                }
            }
        };
    }

    public CachingReaderFactory getCachingReaderFactory() {
        if (cachingReaderFactory == null) {
            cachingReaderFactory = new CachingReaderFactory(configuration);
        }
        return cachingReaderFactory;
    }

    public ReaderFactory getReaderFactory() {
        if (readerFactory == null) {
            readerFactory = new ReaderFactory(configuration);
        }
        return readerFactory;
    }

    public ReaderFactoryPool getReaderFactoryPool() {
        if (readerFactoryPool == null) {
            readerFactoryPool = new ReaderFactoryPool(configuration, 4);
        }
        return readerFactoryPool;
    }

    public WriterFactory getWriterFactory() {
        if (writerFactory == null) {
            writerFactory = new WriterFactory(configuration);
        }
        return writerFactory;
    }
}