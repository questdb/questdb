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

import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Randomised coverage for PER-CELL {@code DROP PARTITION} — the capability sub-project 1C added.
 * <p>
 * <b>Why this exists rather than a fuzz generator.</b> {@code CompositeFuzzRunner} applies every
 * operation UNCHANGED TO BOTH TWINS and asserts twin-equality; its {@code Support} enum has exactly
 * two values, "safe on both twins" and "must not be applied". Per-cell drop fits neither, because it
 * is the project's first COMPOSITE-ONLY capability: a plain day IS its single cell, so no plain
 * statement removes a subset of a day. There is nothing to apply to the reference twin, so
 * twin-equality cannot be the oracle here — a twin-differential generator for this shape cannot be
 * written, not merely has not been.
 * <p>
 * <b>The oracle used instead.</b> An in-memory model of the rows. After dropping cell
 * {@code (day D, value V)} the table must contain exactly the model minus every row with
 * {@code day(ts) == D AND exch == V}. That is expressible without a plain twin, and it checks the
 * property the statement actually promises: remove what was named, and nothing else.
 */
public class CompositePerCellDropPropertyTest extends AbstractCairoTest {

    private static final String[] EXCHANGES = {"E0", "E1", "E2", "E3"};

    @Test(timeout = 120_000)
    public void testRandomPerCellDropsMatchTheModel() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY, exch LAYOUT PLAIN WAL");

            // model: one entry per row, as (day, exch)
            final List<String> model = new ArrayList<>();
            final StringBuilder values = new StringBuilder();
            for (int day = 1; day <= 5; day++) {
                for (int i = 0, n = 2 + rnd.nextInt(3); i < n; i++) {
                    final String exch = EXCHANGES[rnd.nextInt(EXCHANGES.length)];
                    final int hour = 1 + rnd.nextInt(20);
                    if (values.length() > 0) {
                        values.append(',');
                    }
                    values.append("('2023-01-0").append(day).append('T')
                            .append(String.format("%02d", hour)).append(":00:00.000000Z','")
                            .append(exch).append("',1.0)");
                    model.add(day + "/" + exch);
                }
            }
            execute("INSERT INTO c VALUES " + values);
            drainWalQueue();
            assertModel(model);

            // drop random attached cells, verifying the model after each
            final int rowsBefore = model.size();
            int dropsApplied = 0;
            for (int round = 0; round < 8 && !model.isEmpty(); round++) {
                final String victim = model.get(rnd.nextInt(model.size()));
                final String day = victim.substring(0, victim.indexOf('/'));
                final String exch = victim.substring(victim.indexOf('/') + 1);

                execute("ALTER TABLE c DROP PARTITION LIST '2023-01-0" + day + '/' + exch + '\'');
                drainWalQueue();

                // the oracle: every row of that (day, cell) goes, and nothing else
                model.removeIf(e -> e.equals(victim));
                dropsApplied++;
                assertModel(model);
            }

            // Anti-vacuity: the model comparison passes trivially if nothing was ever dropped. Require
            // that drops actually happened AND actually removed rows -- a run where every DROP was a
            // silent no-op would otherwise be indistinguishable from a correct one.
            org.junit.Assert.assertTrue("no per-cell drops were applied -- the test is vacuous",
                    dropsApplied > 0);
            org.junit.Assert.assertTrue("drops removed no rows -- the test is vacuous (before="
                            + rowsBefore + ", after=" + model.size() + ')',
                    model.size() < rowsBefore);
        });
    }

    /**
     * Asserts the table's contents equal the model, as a sorted {@code day/exch} multiset. Compared as
     * counts per (day, exch) rather than raw rows, because that is exactly the granularity a per-cell
     * drop operates at — a mismatch here means the statement removed the wrong cell, or too many.
     */
    private void assertModel(List<String> model) throws Exception {
        final StringSink expected = new StringSink();
        expected.put("k\tc\n");
        model.stream().sorted().distinct().forEach(k -> {
            final long n = model.stream().filter(k::equals).count();
            expected.put(k).put('\t').put(n).put('\n');
        });

        final StringSink actual = new StringSink();
        printSql("select concat(cast(day(ts) as string), '/', exch) k, count() c from c order by k", actual);
        TestUtils.assertEquals(expected, actual);
    }
}
