/*+*****************************************************************************
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

import io.questdb.PropertyKey;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class SpliceJoinMapGrowthTest extends AbstractCairoTest {
    private static final int ROWS = 1_024;

    @Test
    public void testCompositeKey() throws Exception {
        testMapGrowth("k, id");
    }

    @Test
    public void testLongKey() throws Exception {
        testMapGrowth("id");
    }

    @Test
    public void testNonDeterministicKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a AS (SELECT x AS id, x::TIMESTAMP ts FROM long_sequence(4)) TIMESTAMP(ts)");
            execute("CREATE TABLE b AS (SELECT x AS id, x::TIMESTAMP ts FROM long_sequence(4)) TIMESTAMP(ts)");
            try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1) {
                private long ticks;

                @Override
                public long getMicrosecondTimestamp() {
                    return ++ticks;
                }
            }.with(sqlExecutionContext.getSecurityContext())) {
                // Every key is distinct, even when the same record is read again.
                assertQuery("SELECT a.id AS aid, b.id AS bid"
                        + " FROM (SELECT id, systimestamp() AS k, ts FROM a) a"
                        + " SPLICE JOIN (SELECT id, systimestamp() AS k, ts FROM b) b ON (k)"
                        + " WHERE a.id IS NOT NULL")
                        .withContext(context)
                        .noRandomAccess()
                        .sizeMayVary()
                        .withPlanContaining("Splice Join")
                        .returns("""
                                aid\tbid
                                1\tnull
                                2\tnull
                                3\tnull
                                4\tnull
                                """);
            }
        });
    }

    private void testMapGrowth(String keys) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_KEY_CAPACITY, 4);
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_PAGE_SIZE, 128);
        setProperty(PropertyKey.CAIRO_SQL_UNORDERED_MAP_MAX_ENTRY_SIZE, 128);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a AS (SELECT 2 * x AS id, x AS k, x::TIMESTAMP ts"
                    + " FROM long_sequence(" + ROWS + ")) TIMESTAMP(ts)");
            execute("CREATE TABLE b AS (SELECT 2 * x + x % 2 AS id, x AS k, x::TIMESTAMP ts"
                    + " FROM long_sequence(" + ROWS + ")) TIMESTAMP(ts)");

            // A new slave key can move the map while the master row is pending.
            StringSink expected = new StringSink();
            expected.put("aid\tbid\n");
            for (int i = 1; i <= ROWS; i++) {
                expected.put(2 * i).put('\t');
                if (i % 2 == 0) {
                    expected.put(2 * i);
                } else {
                    expected.put("null");
                }
                expected.put('\n');
            }
            assertQuery("SELECT a.id AS aid, b.id AS bid FROM a SPLICE JOIN b ON (" + keys + ") WHERE a.id IS NOT NULL")
                    .noRandomAccess()
                    .sizeMayVary()
                    .withPlanContaining("Splice Join")
                    .returns(expected);
        });
    }
}
