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

package io.questdb.test.cairo.covering;

import org.junit.Test;

public class CoveringIndexPlanTest extends AbstractCoveringIndexQueryTest {

    @Test
    public void testPlanShowsIntervalUnderCoveringIndex() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetry();

            assertQuery("SELECT param_id, max(value) FROM telemetry" +
                    " WHERE param_id IN ('SFID','HOTMIC')" +
                    " AND ts >= '1970-01-01T00:00:01.000000Z'" +
                    " AND ts < '1970-01-01T00:00:05.000000Z'")
                    .noLeakCheck()
                    .assertsPlan(
                            "GroupBy vectorized: true workers: 1\n" +
                                    "  keys: [param_id]\n" +
                                    "  values: [max(value)]\n" +
                                    "    CoveringIndex on: param_id with: value, ts\n" +
                                    "      frames: per-key (unordered)\n" +
                                    "      filter: param_id IN ['SFID','HOTMIC']\n" +
                                    "        Interval forward scan on: telemetry\n" +
                                    "          intervals: [(\"1970-01-01T00:00:01.000000Z\",\"1970-01-01T00:00:04.999999Z\")]\n"
                    );
        });
    }

    @Test
    public void testOrderInsensitiveAggregateStillTakesPerKey() throws Exception {
        assertMemoryLeak(() -> {
            createTelemetry();

            // max() is order-insensitive, so the offer is accepted and the plan says per-key.
            assertQuery("SELECT param_id, max(value) FROM telemetry WHERE param_id IN ('SFID','HOTMIC')")
                    .noLeakCheck()
                    .assertsPlan(
                            "GroupBy vectorized: true workers: 1\n" +
                                    "  keys: [param_id]\n" +
                                    "  values: [max(value)]\n" +
                                    "    CoveringIndex on: param_id with: value\n" +
                                    "      frames: per-key (unordered)\n" +
                                    "      filter: param_id IN ['SFID','HOTMIC']\n" +
                                    "        Frame forward scan on: telemetry\n"
                    );
        });
    }
}
