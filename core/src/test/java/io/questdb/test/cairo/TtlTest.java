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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class TtlTest extends AbstractCairoTest {

    @Test
    public void testExactlyAtTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1");
        execute("INSERT INTO tango VALUES (0), (3_600_000_000), (7_200_000_000)");
        assertQuery("ts\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T01:00:00.000000Z\n" +
                        "1970-01-01T02:00:00.000000Z\n",
                "tango", "ts", true, true);
    }

    @Test
    public void testOneMicrosBeyondTtl() throws Exception {
        execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR TTL 1");
        execute("INSERT INTO tango VALUES (0), (3_600_000_000), (7_200_000_001)");
        assertQuery("ts\n" +
                        "1970-01-01T01:00:00.000000Z\n" +
                        "1970-01-01T02:00:00.000001Z\n",
                "tango", "ts", true, true);
    }
}
