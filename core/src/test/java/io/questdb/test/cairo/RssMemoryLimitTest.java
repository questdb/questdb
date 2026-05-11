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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.log.LogFactory;
import io.questdb.std.MemoryTag;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RssMemoryLimitTest extends AbstractCairoTest {

    private static final LogCapture capture = new LogCapture();

    @Override
    public void setUp() {
        LogFactory.enableGuaranteedLogging(QueryProgress.class);
        super.setUp();
        capture.start();
    }

    @After
    public void tearDown() throws Exception {
        capture.stop();
        super.tearDown();
        LogFactory.disableGuaranteedLogging(QueryProgress.class);
    }

    @Test
    public void testCreateAtomicTable() throws Exception {
        long limitMiB = 2;
        assertMemoryLeak(limitMiB, () -> {
            try {
                execute("create atomic table x as (select" +
                        " rnd_timestamp(to_timestamp('2024-03-01', 'yyyy-mm-dd'), to_timestamp('2024-04-01', 'yyyy-mm-dd'), 0) ts" +
                        " from long_sequence(1000000)) timestamp(ts) partition by day;");
                fail("Managed to create table with RSS limit " + limitMiB + " MB");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "global RSS memory limit exceeded");
            }
            capture.waitForRegex("QueryProgress err .*memoryTag=" + MemoryTag.NATIVE_O3);
            capture.assertLoggedRE(" exe \\[.*, sql=`create atomic table x as \\(select rnd_timestamp\\(to_timestamp\\(");
            capture.assertLoggedRE(" err \\[.*, sql=`create atomic table x as \\(select rnd_timestamp\\(to_timestamp\\(");
        });
    }

    @Test
    public void testLargeTxEventuallySucceeds() throws Exception {
        long limitMiB = 60;
        assertMemoryLeak(limitMiB, () -> {
            int batchCount = 10;
            int batchSize = 500_000;

            execute("create table x (ts timestamp, i int, l long, d double, vch varchar) timestamp(ts) partition by day wal;");

            for (int i = 0; i < batchCount; i++) {
                execute("insert into x select" +
                        " rnd_timestamp('2024-01-01', '2025-01-01', 0) ts," +
                        " rnd_int(), rnd_long(), rnd_double(), rnd_varchar(1, 50, 0)" +
                        " from long_sequence(" + batchSize + ");");
                System.out.println("Tx no. " + i + " done -----");
            }

            TableToken tt = engine.getTableTokenIfExists("x");

            int expectedRowCount = batchCount * batchSize;
            TestUtils.assertEventually(() -> {
                drainWalQueue();
                assertTableNotSuspended();
                assertTrue(engine.getTableSequencerAPI().getTxnTracker(tt).getMemPressureControl().isReadyToProcess());

                try {
                    // cannot use assertQuery, because it clears CairoEngine - this clears all seqTxnTrackers
                    // and we lose information about memory pressure
                    assertQueryFullFatNoLeakCheck("count\n" +
                                    expectedRowCount + "\n",
                            "select count() from x", null, false, true, false);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, 600);

        });
    }

    @Test
    public void testSelect() throws Exception {
        assertMemoryLeak(14, () -> {
            execute("create table test as (select rnd_str() a, rnd_double() b from long_sequence(1000000))");
            assertException(
                    "select a, sum(b) from test",
                    0,
                    "global RSS memory limit exceeded"
            );
            capture.waitForRegex("QueryProgress err .*memoryTag=" + MemoryTag.NATIVE_FAST_MAP_INT_LIST);
            capture.assertLoggedRE(" exe \\[.*, sql=`select a, sum\\(b\\) from test");
            capture.assertLoggedRE(" err \\[.*, sql=`select a, sum\\(b\\) from test");
        });
    }

    @Test
    public void testTooLargeTxEventuallyGivesUp() throws Exception {
        // even reducing parallelism still won't help with extreme transaction
        // this tests that we eventually give up and suspend the table
        long limitMiB = 1;
        assertMemoryLeak(limitMiB, () -> {
            int batchCount = 100;
            int batchSize = 50_000;

            execute("create table x (ts timestamp) timestamp(ts) partition by day wal;");

            for (int i = 0; i < batchCount; i++) {
                execute("insert into x select" +
                        " rnd_timestamp(to_timestamp('2024-01-01', 'yyyy-mm-dd'), to_timestamp('2025-01-01', 'yyyy-mm-dd'), 0) ts" +
                        " from long_sequence(" + batchSize + ");");
            }

            TestUtils.assertEventually(() -> {
                drainWalQueue();
                assertTableSuspended();
            }, 600);
        });
    }

    /**
     * End-to-end check that ALTER TABLE ... ADD INDEX TYPE POSTING on a
     * large partition does not OOM under a tight RSS limit. Without the
     * spill back-pressure introduced alongside this test, a 200_000 row
     * partition with a small spill budget would accumulate the entire
     * partition's row IDs in anonymous heap before the seal, blowing the
     * RSS_MEM_LIMIT during spillKey's per-key buffer doubling. With the
     * back-pressure, mid-stream flushes drain into mmap'd .pv files and
     * the indexing run completes within budget. Queries against the
     * index must still return correct row counts after the alter.
     */
    @Test
    public void testAlterAddPostingIndexUnderRssLimit() throws Exception {
        // 1 MiB spill budget forces the periodic flush to fire repeatedly
        // during ALTER ADD INDEX. The default budget would absorb the
        // whole 200_000-row partition and never fire.
        node1.getConfigurationOverrides().setProperty(
                PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 1L * 1024L * 1024L);
        // 96 MiB RSS limit: large enough for create+insert and the post-fix
        // indexing path (which holds at most ~1 MiB of spill at a time);
        // far too small for the unbounded pre-fix path (which would peak
        // at ~1.6 MiB raw + doubling overhead per hot key, well past the
        // limit when summed across a few hundred keys).
        long limitMiB = 96;
        assertMemoryLeak(limitMiB, () -> {
            execute("create table t (ts timestamp, sym symbol, v double) timestamp(ts) partition by day wal;");
            execute("insert into t select " +
                    " timestamp_sequence('2024-01-01T00:00:00.000000Z', 1) ts," +
                    " rnd_symbol('A','B','C','D','E','F','G','H','I','J') sym," +
                    " rnd_double() v" +
                    " from long_sequence(200_000)");
            drainWalQueue();
            execute("alter table t alter column sym add index type posting");
            drainWalQueue();
            assertSql("count\n200000\n", "select count() from t where sym = 'A' or sym = 'B' or sym = 'C' or sym = 'D' or sym = 'E' or sym = 'F' or sym = 'G' or sym = 'H' or sym = 'I' or sym = 'J'");
        });
    }

    private static void assertTableNotSuspended() {
        TableToken tt = engine.getTableTokenIfExists("x");
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
    }

    private static void assertTableSuspended() {
        TableToken tt = engine.getTableTokenIfExists("x");
        Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tt));
    }
}
