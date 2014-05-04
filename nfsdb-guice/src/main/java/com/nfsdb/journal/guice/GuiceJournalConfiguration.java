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

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.nfsdb.journal.factory.JournalConfiguration;

import java.io.File;

public class GuiceJournalConfiguration extends JournalConfiguration {

    @Inject
    public GuiceJournalConfiguration(ConfigHolder holder) {
        super(holder.config, holder.base);
    }

    static class ConfigHolder {

        @Inject(optional = true)
        @Named("nfsdb.config")
        private final String config = "/nfsdb.xml";

        @Inject
        @Named("nfsdb.base")
        private File base;
    }
}
