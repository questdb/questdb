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

package io.questdb.test.cairo;

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Follow-up 2 — pins the dependency between {@code CellRegistry#getTupleFromWriter} and the NARROW
 * null guard in {@code MapWriter#valueOf}.
 * <p>
 * {@code MapWriter#valueOf} returns null only for {@code SymbolTable.VALUE_IS_NULL}. That looks like an
 * oversight next to the reader's {@code key > -1 && key < symbolCount} shape, and widening it to match
 * is a natural-looking tidy-up. It is not safe: {@code getTupleFromWriter} reads back an ordinal the
 * same writer JUST interned, before the writer's committed symbol count covers it, and relies on the
 * narrow guard letting that read through. Widening it was measured to SUSPEND a composite table on
 * three ordinary distinct dimension values in a single commit, with no NULL involved at all.
 * <p>
 * These tests are the regression net for that. They exercise the exact traffic that broke — several
 * fresh dimension values interned within one commit, so the registry decodes ordinals the writer has
 * not yet committed — and assert the table survives and reads correctly. Widen the guard and they fail.
 * <p>
 * The failure mode is why this is worth pinning: nothing about the symptom (a suspended table on
 * ordinary data) points at a null check for NULL symbols.
 */
public class CompositeCellRegistryToleranceTest extends AbstractCairoTest {

    /**
     * The measured trigger: three distinct dimension values, all new, all in ONE commit. Each is
     * interned and immediately decoded back through the writer.
     */
    @Test
    public void testThreeFreshDimensionValuesInOneCommit() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("insert into c values ('2023-01-01T00:00:00.000000Z','BTC',1.0),"
                    + "('2023-01-01T00:00:01.000000Z','ETH',2.0),"
                    + "('2023-01-01T00:00:02.000000Z','SOL',3.0)");
            drainWalQueue();

            assertNotSuspended();
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");
            assertQuery("select exch, count() from c order by exch").noLeakCheck().expectSize()
                    .returns("exch\tcount\nBTC\t1\nETH\t1\nSOL\t1\n");
        });
    }

    /**
     * Wider version of the same shape: enough fresh values in one commit that the registry is
     * repeatedly reading ordinals ahead of the writer's committed count, plus repeats of values
     * already interned in the same commit.
     */
    @Test
    public void testManyFreshDimensionValuesWithRepeatsInOneCommit() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final StringBuilder sql = new StringBuilder("insert into c values ");
            final int distinct = 24;
            final int rows = distinct * 3;
            for (int i = 0; i < rows; i++) {
                if (i > 0) {
                    sql.append(',');
                }
                // i % distinct means every value repeats, so the interner takes both the
                // "new ordinal" and the "already seen" path within one commit.
                sql.append("('2023-01-01T00:00:00.0000").append(String.format("%02d", i))
                        .append("Z','S").append(i % distinct).append("',").append(i).append(".0)");
            }
            execute(sql.toString());
            drainWalQueue();

            assertNotSuspended();
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n" + rows + "\n");
            assertQuery("select count_distinct(exch) from c").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count_distinct\n" + distinct + "\n");
        });
    }

    /**
     * The narrow guard's actual purpose still has to work: a NULL dimension value is a legitimate
     * cell, not an error. This is the half of the behaviour that a widened guard would preserve —
     * kept here so the pin above cannot be "fixed" by deleting the guard outright either.
     */
    @Test
    public void testNullDimensionValueAlongsideFreshValues() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("insert into c values ('2023-01-01T00:00:00.000000Z','BTC',1.0),"
                    + "('2023-01-01T00:00:01.000000Z',null,2.0),"
                    + "('2023-01-01T00:00:02.000000Z','ETH',3.0)");
            drainWalQueue();

            assertNotSuspended();
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");
            assertQuery("select count() from c where exch is null").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n1\n");
        });
    }

    private void assertNotSuspended() {
        Assert.assertFalse(
                "composite table must not be suspended",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c")));
    }

    private void createTable() throws SqlException {
        execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
    }
}
