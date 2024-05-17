/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.cairo;

import io.questdb.griffin.SqlException;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RssMemoryLimitTest extends AbstractCairoTest {

    @After
    public void tearDown() throws Exception {
        Unsafe.setRssMemLimit(0);
        super.tearDown();
    }

    @Test
    public void testCreateAtomicTable() throws Exception {
        long limitMB = 12;
        Unsafe.setRssMemLimit(limitMB * 1_000_000);
        assertMemoryLeak(() -> {
            try {
                ddl("create atomic table x as (select" +
                        " rnd_timestamp(to_timestamp('2024-03-01', 'yyyy-mm-dd'), to_timestamp('2024-04-01', 'yyyy-mm-dd'), 0) ts" +
                        " from long_sequence(10000000)) timestamp(ts) partition by day;");
                fail("Managed to create table with RSS limit " + limitMB + " MB");
            } catch (SqlException e) {
                String expected = "global RSS memory limit exceeded";
                assertTrue(String.format("Exception should contain \"%s\", but was \"%s\"", expected, e.getMessage()),
                        e.getMessage().contains(expected));
                drop("drop table x;");
            }
        });
    }
}
