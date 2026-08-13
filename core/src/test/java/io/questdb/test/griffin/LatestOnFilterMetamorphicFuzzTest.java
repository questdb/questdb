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

package io.questdb.test.griffin;

import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * Metamorphic fuzz for predicate loss around LATEST ON.
 * <p>
 * WHERE is documented to run before LATEST ON at the same query level, so pushing the predicate
 * into a sub-query must not change the result:
 * <pre>
 *     SELECT * FROM t WHERE p LATEST ON ts PARTITION BY k
 *  ==
 *     SELECT * FROM (SELECT * FROM t WHERE p) LATEST ON ts PARTITION BY k
 * </pre>
 * The nested form denies the planner the chance to lift the predicate into
 * {@code IntrinsicModel.keyColumn}, so the pair is exactly sensitive to a predicate being extracted
 * for an index-backed cursor that is never built. That is the failure this catches: the flat form
 * silently returning rows the WHERE clause excludes.
 * <p>
 * The generator deliberately varies the two things that decide whether extraction happens and
 * whether anything consumes it: whether the predicate column is indexed, and the type of the
 * LATEST ON key.
 * <p>
 * The nested form is only a valid oracle while the optimizer keeps the sub-query distinct rather
 * than flattening it up into the LATEST ON model; LATEST ON acts as a merge barrier today, and the
 * fuzz fails on a fix-reverted build, which is what shows the pair still discriminates. The
 * no_index form is checked alongside it so the test does not rest on that property alone.
 */
public class LatestOnFilterMetamorphicFuzzTest extends AbstractCairoTest {

    private static final String[] KEY_COLUMNS = {"key_varchar", "key_long", "key_sym", "key_uuid"};
    private static final int ROWS = 400;

    @Test
    public void testFlatMatchesNested() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            for (int iteration = 0; iteration < 40; iteration++) {
                final String table = "fuzz_" + iteration;
                final boolean isIndexed = rnd.nextBoolean();
                createTable(table, isIndexed, rnd);

                final String key = KEY_COLUMNS[rnd.nextInt(KEY_COLUMNS.length)];
                final String predicate = randomPredicate(rnd);
                final String latestOn = " latest on ts partition by " + key;

                // both forms are ordered identically so cursor comparison is stable
                final String flat = "select * from (select * from " + table
                        + " where " + predicate + latestOn + ") order by ts, " + key;
                final String nested = "select * from (select * from (select * from " + table
                        + " where " + predicate + ")" + latestOn + ") order by ts, " + key;
                // a second, independent oracle: the hint blocks extraction at the same query level,
                // so this one holds even if the planner ever flattens the sub-query above
                final String hinted = "select * from (select /*+ no_index(" + table + " sym) */ * from "
                        + table + " where " + predicate + latestOn + ") order by ts, " + key;

                try {
                    assertSqlCursors(nested, flat);
                    assertSqlCursors(hinted, flat);
                } catch (AssertionError e) {
                    throw new AssertionError("flat form dropped a predicate: indexed=" + isIndexed
                            + ", key=" + key + ", predicate=" + predicate + "\nflat=" + flat + "\n" + e, e);
                }
            }
        });
    }

    private void createTable(String table, boolean isIndexed, Rnd rnd) throws Exception {
        execute("create table " + table + " (" +
                "sym symbol" + (isIndexed ? " index" : "") + ", " +
                "other_sym symbol, " +
                "flag boolean, " +
                "key_varchar varchar, " +
                "key_long long, " +
                "key_sym symbol, " +
                "key_uuid uuid, " +
                "ts timestamp" +
                ") timestamp(ts) partition by day wal");

        final int distinctKeys = 1 + rnd.nextInt(8);
        final StringBuilder sb = new StringBuilder("insert into " + table + " values ");
        for (int i = 0; i < ROWS; i++) {
            if (i > 0) {
                sb.append(',');
            }
            final int k = rnd.nextInt(distinctKeys);
            sb.append("('s").append(rnd.nextInt(5)).append('\'')
                    .append(",'o").append(rnd.nextInt(4)).append('\'')
                    .append(',').append(rnd.nextBoolean())
                    .append(",'k").append(k).append('\'')
                    .append(',').append(k)
                    .append(",'k").append(k).append('\'')
                    // a stable uuid per key value
                    .append(",'00000000-0000-0000-0000-00000000000").append(k).append('\'')
                    // spread over several partitions
                    .append(",").append(i * 3_600_000_000L)
                    .append(')');
        }
        execute(sb.toString());
        drainWalQueue();
    }

    private String randomPredicate(Rnd rnd) {
        final String col = rnd.nextBoolean() ? "sym" : "other_sym";
        final String v1 = "'s" + rnd.nextInt(6) + "'";
        final String v2 = "'s" + rnd.nextInt(6) + "'";
        switch (rnd.nextInt(7)) {
            case 0:
                return col + " = " + v1;
            case 1:
                return col + " != " + v1;
            case 2:
                return col + " in (" + v1 + ", " + v2 + ")";
            case 3:
                return col + " not in (" + v1 + ", " + v2 + ")";
            case 4:
                return col + " = " + v1 + " and flag = " + rnd.nextBoolean();
            case 5:
                return col + " in (" + v1 + ") and ts >= 0";
            default:
                return col + " = " + v1 + " or " + col + " = " + v2;
        }
    }
}
