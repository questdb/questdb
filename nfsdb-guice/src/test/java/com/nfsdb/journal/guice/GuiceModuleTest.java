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

package com.nfsdb.journal.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.nfsdb.journal.factory.JournalConfiguration;
import com.nfsdb.journal.factory.JournalFactory;
import com.nfsdb.journal.factory.JournalPool;
import com.nfsdb.journal.factory.JournalReaderFactory;
import com.nfsdb.journal.test.tools.JournalTestFactory;
import com.nfsdb.journal.utils.Files;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

public class GuiceModuleTest {

    @Rule
    public final JournalTestFactory factory = new JournalTestFactory();
    private Injector injector;

    @Before
    public void setUp() throws Exception {
        this.injector = Guice.createInjector(new GuiceJournalModule(), new AbstractModule() {
            @Override
            protected void configure() {
                bind(File.class).annotatedWith(Names.named("nfsdb.base")).toInstance(factory.getConfiguration().getJournalBase());
            }
        });
    }

    @Test
    public void testFactorySingleton() {
        NeedsReaderFactory nf1 = injector.getInstance(NeedsReaderFactory.class);
        NeedsReaderFactory nf2 = injector.getInstance(NeedsReaderFactory.class);

        Assert.assertNotSame(nf1, nf2);
        Assert.assertSame(nf1.getFactory(), nf2.getFactory());
    }

    @Test
    public void testGlobalPoolSingleton() throws Exception {
        NeedsGlobalPool ngp1 = injector.getInstance(NeedsGlobalPool.class);
        NeedsGlobalPool ngp2 = injector.getInstance(NeedsGlobalPool.class);
        Assert.assertNotSame(ngp1, ngp2);
        Assert.assertSame(ngp1.getPool(), ngp2.getPool());
        Assert.assertTrue(ngp1.getPool() instanceof GuiceJournalPool);
    }

    @Test
    public void testPool() throws Exception {
        NeedsPool np1 = injector.getInstance(NeedsPool.class);
        NeedsPool np2 = injector.getInstance(NeedsPool.class);

        Assert.assertNotSame(np1, np2);
        Assert.assertNotSame(np1.getPool(), np2.getPool());
        Assert.assertTrue(np1.getPool() instanceof GuiceJournalPool);
    }

    @Test
    public void testFactoryOverride() throws Exception {
        Injector injector = Guice.createInjector(new GuiceJournalModule(new JournalConfiguration("/db-factory-test.xml", Files.makeTempDir())));
        JournalConfiguration configuration = injector.getInstance(JournalFactory.class).getConfiguration();
        NeedsReaderFactory nf = injector.getInstance(NeedsReaderFactory.class);
        Assert.assertSame(nf.getFactory().getConfiguration(), configuration);

        JournalFactory factory = injector.getInstance(JournalFactory.class);
        factory.close();
        Files.delete(factory.getConfiguration().getJournalBase());
    }


    public static class NeedsReaderFactory {
        @Inject
        private JournalReaderFactory factory;

        @Inject
        public NeedsReaderFactory() {
        }

        public JournalReaderFactory getFactory() {
            return factory;
        }
    }

    public static class NeedsGlobalPool {
        @Inject
        @Named(GuiceJournalModule.GLOBAL_POOL)
        private JournalPool pool;

        @Inject
        public NeedsGlobalPool() {
        }

        public JournalPool getPool() {
            return pool;
        }
    }

    public static class NeedsPool {
        @Inject
        private JournalPool pool;

        @Inject
        public NeedsPool() {
        }

        public JournalPool getPool() {
            return pool;
        }
    }
}
