/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.journal.test.tools;

import com.nfsdb.journal.model.configuration.ModelConfiguration;
import com.nfsdb.journal.utils.Files;
import org.junit.Rule;

public abstract class AbstractTest {
    @Rule
    public final JournalTestFactory factory = new JournalTestFactory(ModelConfiguration.MAIN.build(Files.makeTempDir()));

    public AbstractTest() {
        //System.out.println(this + "created: " + factory.getConfiguration().getJournalBase());
    }
}
