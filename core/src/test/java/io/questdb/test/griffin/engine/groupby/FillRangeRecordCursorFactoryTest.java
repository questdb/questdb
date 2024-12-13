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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class FillRangeRecordCursorFactoryTest extends AbstractCairoTest {
    @Test
    public void testSampleByWithFilterAndOrderByAndLimit() throws Exception {
        assertQuery(
                "open\thigh\tlow\tclose\tvolume\ttimestamp\n" +
                        "22.463013424972587\t90.75843364017028\t16.381374773748515\t75.88175403454873\t440.2232295756601\t1970-01-03T00:00:00.000000Z\n",
                "select * from (" +
                        "  select" +
                        "    first(price) AS open," +
                        "    max(price) AS high," +
                        "    min(price) AS low," +
                        "    last(price) AS close," +
                        "    sum(amount) AS volume," +
                        "    created_at as timestamp" +
                        "  from trades_varchar" +
                        "  where market_id = 'btcusdt' AND created_at > dateadd('m', -60, 172800000000)" +
                        "  sample by 60m" +
                        "  fill(null, null, null, null, 0) align to calendar" +
                        ") order by timestamp desc limit 0, 1",
                "create table trades_varchar as " +
                        "(" +
                        "select" +
                        " rnd_varchar('btcusdt', 'ethusdt') market_id," +
                        " rnd_double(0) * 100 price," +
                        " rnd_double(0) * 100 amount," +
                        " timestamp_sequence(172800000000, 3600000) created_at" +
                        " from long_sequence(20)" +
                        ") timestamp(created_at) partition by day",
                "timestamp###DESC",
                true,
                false
        );
    }
}
