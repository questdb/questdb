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

package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

// Regression test for claim C1: a retained filter containing a tokenless
// (sub-query) node must not NPE in PushdownFilterExtractor when the table has
// parquet-format partitions. Extraction skips the tokenless node (best-effort;
// fewer extracted conditions is always safe) and the filter evaluates normally.
public class PushdownFilterTokenlessNodeTest extends AbstractCairoTest {

    private static final String BOTH_ROWS = "v\tts\n1\t2018-01-01T00:00:00.000000Z\n2\t2018-01-02T00:00:00.000000Z\n";

    @Test
    public void testTokenlessOrOperandOnParquetPartitionNonTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("select * from p where v = 1 or (select b from x limit 1)")
                    .timestamp("ts")
                    .noLeakCheck()
                    .returns(BOTH_ROWS);
            assertQuery("select * from p where v = 1 or (select b from x_false limit 1)")
                    .timestamp("ts")
                    .noLeakCheck()
                    .returns("v\tts\n1\t2018-01-01T00:00:00.000000Z\n");
        });
    }

    @Test
    public void testTokenlessOrOperandOnParquetPartitionTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // the retained-filter shape produced by the WhereClauseParser tokenless-node fix
            assertQuery("select * from p where ts = '2018-01-01' or (select b from x limit 1)")
                    .timestamp("ts")
                    .noLeakCheck()
                    .returns(BOTH_ROWS);
            assertQuery("select * from p where ts = '2018-01-01' or (select b from x_false limit 1)")
                    .timestamp("ts")
                    .noLeakCheck()
                    .returns("v\tts\n1\t2018-01-01T00:00:00.000000Z\n");
        });
    }

    private void createTables() throws Exception {
        execute("CREATE TABLE p (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO p VALUES (1, '2018-01-01T00:00:00.000000Z'), (2, '2018-01-02T00:00:00.000000Z')");
        execute("CREATE TABLE x (b BOOLEAN)");
        execute("INSERT INTO x VALUES (true)");
        execute("CREATE TABLE x_false (b BOOLEAN)");
        execute("INSERT INTO x_false VALUES (false)");
        execute("ALTER TABLE p CONVERT PARTITION TO PARQUET LIST '2018-01-01'");
    }
}
