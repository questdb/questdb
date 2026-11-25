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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * percentile_disc / percentile_cont over groups above the radix selection's size, against the values a
 * sort puts at the percentile's indexes.
 */
public class PercentileLargeGroupTest extends AbstractCairoTest {

    @Test
    public void testLargeGroupsMatchTheSortedValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT rnd_symbol('a', 'b') g, rnd_int(0, 300, 0)::DOUBLE / 4 v, rnd_double() * 1000 - 500 w FROM long_sequence(300_000))");
            for (String col : new String[]{"v", "w"}) {
                for (String g : new String[]{"a", "b"}) {
                    final long n = value("SELECT count() FROM t WHERE g = '" + g + "'");
                    for (double p : new double[]{0.5, 0.37, 0.999}) {
                        final long disc = Math.max(0, (long) Math.ceil(p * n) - 1);
                        Assert.assertEquals(col + " " + g + " disc " + p,
                                valueAt(col, g, disc),
                                dvalue("SELECT percentile_disc(" + col + ", " + p + ") FROM t WHERE g = '" + g + "'"), 0.0);
                        final double pos = p * (n - 1);
                        final long lower = (long) Math.floor(pos);
                        final double lo = valueAt(col, g, lower);
                        final double hi = valueAt(col, g, (long) Math.ceil(pos));
                        Assert.assertEquals(col + " " + g + " cont " + p,
                                lo + (hi - lo) * (pos - lower),
                                dvalue("SELECT percentile_cont(" + col + ", " + p + ") FROM t WHERE g = '" + g + "'"), 1e-9);
                    }
                }
            }
        });
    }

    private static double dvalue(String sql) throws Exception {
        try (RecordCursorFactory f = select(sql); RecordCursor c = f.getCursor(sqlExecutionContext)) {
            Assert.assertTrue(c.hasNext());
            return c.getRecord().getDouble(0);
        }
    }

    private static long value(String sql) throws Exception {
        try (RecordCursorFactory f = select(sql); RecordCursor c = f.getCursor(sqlExecutionContext)) {
            Assert.assertTrue(c.hasNext());
            return c.getRecord().getLong(0);
        }
    }

    private static double valueAt(String col, String g, long index) throws Exception {
        return dvalue("SELECT " + col + " FROM t WHERE g = '" + g + "' ORDER BY " + col + " LIMIT " + index + ", " + (index + 1));
    }
}
