/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.griffin.engine.join;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class JoinPrefixedTablesTest extends AbstractCairoTest {
    @Test
    public void testSimple() throws Exception {
        execute("create table 'a.b.c.d' (tableId int, b int, t timestamp) timestamp(t) partition by day");
        execute("insert into 'a.b.c.d' select x, rnd_int(), timestamp_sequence(0, 1000)");
        execute("create table 'x.y.z' as (select x::int x, rnd_varchar() name from long_sequence(100))");

        assertQuery(
                "tableId\tb\tt\tx\tname\n" +
                        "1\t-1148479920\t1970-01-01T00:00:00.000000Z\t1\t&BT+\n",
                "SELECT * FROM a.b.c.d\n" +
                        "LEFT OUTER JOIN (SELECT x, name from 'x.y.z') t ON t.x = tableId\n",
                "t"
        );
    }
}
