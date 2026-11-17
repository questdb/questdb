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

package io.questdb.test.cairo.composite;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TxReader;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Pins the two properties every opted-in test leans on: the randomiser leaves a real
 * composite partition behind, and it does not move a single row while doing so.
 */
public class CompositePartitionRandomiserTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // The coin flip's heads branch, forced - see AbstractCairoTest#enableCompositePartitionRandomisation.
        isCompositePartitionRandomisationEnabled = true;
        setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, "true");
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
    }

    @Test
    public void testRandomiserLeavesContentUnchanged() throws Exception {
        assertMemoryLeak(() -> {
            final String base = "SELECT x::INT i, ('sym' || (x % 7))::symbol s," +
                    " timestamp_sequence('2020-01-01', 15*1000000L) ts FROM long_sequence(5760)";
            execute("CREATE TABLE x AS (" + base + ") TIMESTAMP(ts) PARTITION BY DAY");

            isCompositePartitionRandomisationEnabled = false;
            execute("CREATE TABLE oracle AS (" + base + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

            // Row for row, not just aggregates - a lost or duplicated row from the
            // round-trip shows up here.
            TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                    "SELECT i, s::varchar s, ts FROM oracle ORDER BY ts, i",
                    "SELECT i, s::varchar s, ts FROM x ORDER BY ts, i",
                    LOG);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                    "SELECT count() c, sum(i) si, min(ts) mn, max(ts) mx FROM oracle",
                    "SELECT count() c, sum(i) si, min(ts) mn, max(ts) mx FROM x",
                    LOG);
        });
    }

    @Test
    public void testRandomiserMakesPartitionComposite() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (" +
                    "SELECT x::INT i, timestamp_sequence('2020-01-01', 15*1000000L) ts FROM long_sequence(5760)" +
                    ") TIMESTAMP(ts) PARTITION BY DAY");

            final TableToken tt = engine.verifyTableName("x");
            try (TableReader reader = engine.getReader(tt)) {
                final TxReader txReader = reader.getTxFile();
                boolean hasComposite = false;
                for (int i = 0, n = txReader.getPartitionCount(); i < n; i++) {
                    hasComposite |= txReader.isPartitionComposite(i);
                }
                Assert.assertTrue("the randomiser should have left a composite partition behind", hasComposite);
            }
        });
    }
}
