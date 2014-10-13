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

package com.nfsdb.journal;

import com.nfsdb.journal.exceptions.JournalConfigurationException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.factory.configuration.JournalConfigurationBuilder;
import com.nfsdb.journal.logging.Logger;
import com.nfsdb.journal.test.tools.JournalTestFactory;
import com.nfsdb.journal.utils.Files;
import com.nfsdb.journal.utils.Rnd;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class HugeTableTest {

    @ClassRule
    public static final JournalTestFactory factory;

    static {
        try {
            factory = new JournalTestFactory(
                    new JournalConfigurationBuilder() {{
                        $(Name.class).recordCountHint(15000000).txCountHint(1)
                                .$sym("name").valueCountHint(15000000).index().noCache()
                        ;
                    }}.build(Files.makeTempDir())
            );
        } catch (JournalConfigurationException e) {
            throw new JournalRuntimeException(e);
        }
    }

    private static final Logger LOGGER = Logger.getLogger(PerformanceTest.class);

    @Test
    public void testLargeSymbolTable() throws Exception {

        JournalWriter<Name> w = factory.writer(Name.class, "name");

        Name name = new Name();
        Rnd rnd = new Rnd();

        long t = 0;
        for (int i = -500000; i < 2000000; i++) {
            if (i == 0) {
                t = System.nanoTime();
            }
            name.name = rnd.nextString(10);
            w.append(name);
        }
        w.commit();

        LOGGER.info("Appended 2M symbols in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t) + "ms");
    }

    public static class Name {
        String name;
    }
}
