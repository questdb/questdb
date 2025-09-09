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

package io.questdb.test.griffin;

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CreateMatViewValidationTest extends AbstractCairoTest {

    @Test
    public void testMatViewWithMatchingTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            // Create base table with ts1 as designated timestamp
            execute("CREATE TABLE y ( " +
                    "x1 INT," +
                    "s SYMBOL CAPACITY 256 CACHE," +
                    "ts1 TIMESTAMP," +
                    "ts2 TIMESTAMP" +
                    ") timestamp(ts1) PARTITION BY DAY WAL");

            // This should succeed - using the same timestamp as base table
            execute("CREATE MATERIALIZED VIEW y_view_valid AS " +
                    "SELECT ts1, sum(x1) as sum_x1 FROM y TIMESTAMP(ts1) SAMPLE BY 2s");
        });
    }

    @Test
    public void testMatViewWithMismatchedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            // Create base table with ts1 as designated timestamp
            execute("CREATE TABLE y ( " +
                    "x1 INT," +
                    "s SYMBOL CAPACITY 256 CACHE," +
                    "ts1 TIMESTAMP," +
                    "ts2 TIMESTAMP" +
                    ") timestamp(ts1) PARTITION BY DAY WAL");

            // This should fail - using different timestamp than base table
            try {
                execute("CREATE MATERIALIZED VIEW y_view_invalid AS " +
                        "SELECT ts2, sum(x1) as sum_x1 FROM y TIMESTAMP(ts2) SAMPLE BY 2s");
                fail("Expected SqlException");
            } catch (SqlException e) {
                assertTrue(e.getMessage().contains("materialized view query timestamp must match base table designated timestamp"));
                assertTrue(e.getMessage().contains("base table timestamp=ts1"));
                assertTrue(e.getMessage().contains("materialized view timestamp=ts2"));
            }
        });
    }

    @Test
    public void testMatViewWithImplicitTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            // Create base table with ts1 as designated timestamp
            execute("CREATE TABLE y ( " +
                    "x1 INT," +
                    "s SYMBOL CAPACITY 256 CACHE," +
                    "ts1 TIMESTAMP," +
                    "ts2 TIMESTAMP" +
                    ") timestamp(ts1) PARTITION BY DAY WAL");

            // This should succeed - no explicit timestamp, using base table's timestamp
            execute("CREATE MATERIALIZED VIEW y_view_implicit AS " +
                    "SELECT ts1, sum(x1) as sum_x1 FROM y SAMPLE BY 2s");
        });
    }

    @Test
    public void testMatViewWithNoBaseTableTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            // Create base table without designated timestamp but with WAL enabled
            execute("CREATE TABLE y_no_ts ( " +
                    "x1 INT," +
                    "s SYMBOL CAPACITY 256 CACHE," +
                    "ts1 TIMESTAMP," +
                    "ts2 TIMESTAMP" +
                    ") WAL");

            // This should succeed - no base table timestamp to validate against
            execute("CREATE MATERIALIZED VIEW y_view_no_base_ts AS " +
                    "SELECT ts1, sum(x1) as sum_x1 FROM y_no_ts TIMESTAMP(ts1) SAMPLE BY 2s");
        });
    }
}
