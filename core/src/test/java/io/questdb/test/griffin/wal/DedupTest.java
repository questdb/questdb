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

package io.questdb.test.griffin.wal;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class DedupTest extends AbstractCairoTest {
    @Test
    public void testDedupVarcharSimple() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x (s varchar," +
                    "               x int," +
                    "               ts timestamp) timestamp(ts) partition by DAY WAL" +
                    "               DEDUP UPSERT KEYS(ts, s)");

            // Insert data
            insert("insert into x(s, x, ts)" +
                    " values ('a', 01, '2020-01-01T00:00:00.000Z')," +
                    "        ('c', 10, '2020-01-03T00:00:00.000Z')," +
                    "        ('b', 02, '2020-01-01T00:00:00.000Z')," +
                    "        ('a', 03, '2020-01-01T00:00:00.000Z')," +
                    "        ('c', 20, '2020-01-03T00:00:00.000Z');"
            );

            drainWalQueue();

            assertSql(
                    "s\tx\tts\n" +
                            "a\t3\t2020-01-01T00:00:00.000000Z\n" +
                            "b\t2\t2020-01-01T00:00:00.000000Z\n" +
                            "c\t20\t2020-01-03T00:00:00.000000Z\n",
                    "select * from x order by ts, s"
            );

            // Insert partially matching data
            insert("insert into x(s, x, ts)" +
                    " values ('a', 31, '2020-01-01T00:00:00.000Z')," +
                    "        ('c', 32, '2020-01-04T00:00:00.000Z')," +
                    "        ('b', 33, '2020-01-01T00:00:00.000Z')," +
                    "        ('a', 34, '2020-01-01T00:00:00.000Z')," +
                    "        ('c', 35, '2020-01-03T00:00:00.000Z');"
            );

            drainWalQueue();

            assertSql(
                    "s\tx\tts\n" +
                            "a\t3\t2020-01-01T00:00:00.000000Z\n" +
                            "b\t2\t2020-01-01T00:00:00.000000Z\n" +
                            "c\t20\t2020-01-03T00:00:00.000000Z\n",
                    "select * from x order by ts, s"
            );
        });
    }
}
