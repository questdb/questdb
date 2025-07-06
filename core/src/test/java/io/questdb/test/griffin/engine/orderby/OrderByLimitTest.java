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

package io.questdb.test.griffin.engine.orderby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class OrderByLimitTest extends AbstractCairoTest {
    @Test
    public void testNegativeLimitDescOrderBy() throws Exception {
        assertQuery(
                "price\tts\n" +
                        "0.6607777894187332\t2024-01-01T00:00:00.000000Z\n",
                "select price, ts " +
                        "from x " +
                        "where price > 0 AND ts >= '2024-01-01' AND ts <= '2024-12-31' " +
                        "order by ts desc limit -1",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double() price," +
                        " timestamp_sequence('2024', 24*60*60*1000*1000) ts" +
                        " from" +
                        " long_sequence(2)" +
                        ") timestamp(ts) partition by day",
                "ts###desc",
                true,
                true
        );
    }
}
