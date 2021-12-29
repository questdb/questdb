/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin;

import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class WithClauseTest extends AbstractGriffinTest {
    @Test
    public void testWithLatestByFilterGroup() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table contact_events2 as (\n" +
                    "  select cast(x as SYMBOL) _id,\n" +
                    "    rnd_symbol('c1', 'c2', 'c3', 'c4') contactid, \n" +
                    "    CAST(x as Timestamp) timestamp, \n" +
                    "    rnd_symbol('g1', 'g2', 'g3', 'g4') groupId \n" +
                    "from long_sequence(500)) \n" +
                    "timestamp(timestamp)", sqlExecutionContext);

            // this is deliberately shuffled column in select to check that correct metadata is used on filtering
            // latest by queries
            String expected = select("select groupId, _id, contactid, timestamp, _id from contact_events2 where groupId = 'g1' latest on timestamp partition by _id order by timestamp");
            Assert.assertTrue(expected.length() > 100);

            assertQuery(expected,
                    "with eventlist as (\n" +
                            "    select * from contact_events2 where groupId = 'g1' latest on timestamp partition by _id order by timestamp\n" +
                            ")\n" +
                            "select groupId, _id, contactid, timestamp, _id from eventlist where groupId = 'g1' \n",
                    "timestamp", true, false, true);
        });
    }

    private String select(CharSequence selectSql) throws SqlException {
        TestUtils.printSql(
                compiler,
                sqlExecutionContext,
                selectSql,
                sink
        );
        return sink.toString();
    }
}
