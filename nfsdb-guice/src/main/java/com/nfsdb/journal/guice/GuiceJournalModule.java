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
import com.google.inject.name.Names;
import com.nfsdb.journal.factory.*;

public class GuiceJournalModule extends AbstractModule {

    public static final String GLOBAL_POOL = "nfsdb-global-pool";
    // overriding factory
    private final JournalConfiguration configuration;

    public GuiceJournalModule() {
        configuration = null;
    }

    public GuiceJournalModule(JournalConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    protected void configure() {

        if (configuration != null) {
            bind(JournalConfiguration.class).toInstance(configuration);
        } else {
            bind(JournalConfiguration.class).to(GuiceJournalConfiguration.class).asEagerSingleton();
        }
        bind(JournalReaderFactory.class).to(GuiceJournalFactory.class).asEagerSingleton();
        bind(JournalWriterFactory.class).to(GuiceJournalFactory.class).asEagerSingleton();
        bind(JournalFactory.class).to(GuiceJournalFactory.class).asEagerSingleton();
        bind(JournalPool.class).annotatedWith(Names.named(GLOBAL_POOL)).to(GuiceJournalPool.class).asEagerSingleton();
        bind(JournalPool.class).to(GuiceJournalPool.class);
    }
}
