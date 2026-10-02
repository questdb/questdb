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

package io.questdb.test.griffin.engine.window;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

public class SdtReconstructionBoundTest extends AbstractCairoTest {
    @Test
    public void testMicrosecondReconstructionBound() throws Exception {
        assertReconstructionBound("TIMESTAMP");
    }

    @Test
    public void testNanosecondReconstructionBound() throws Exception {
        assertReconstructionBound("TIMESTAMP_NS");
    }

    private static void assertExactError(String query, double[] stored, boolean isWindow) throws SqlException {
        try (RecordCursorFactory factory = select(query)) {
            // Reopen the same factory, and replay each cursor. The oracle uses stored DOUBLEs,
            // not SQL's printed decimal values or a floating-point interpolation at a large offset.
            for (int open = 0; open < 2; open++) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    for (int pass = 0; pass < 2; pass++) {
                        if (pass > 0) {
                            cursor.toTop();
                        }
                        boolean[] isKept = new boolean[stored.length];
                        Record record = cursor.getRecord();
                        while (cursor.hasNext()) {
                            int index = (int) record.getTimestamp(0);
                            Assert.assertEquals(Double.doubleToRawLongBits(stored[index]), Double.doubleToRawLongBits(record.getDouble(1)));
                            isKept[index] = !isWindow || record.getBool(2);
                        }
                        Assert.assertTrue(isKept[0]);
                        Assert.assertTrue(isKept[stored.length - 1]);
                        int left = 0;
                        for (int right = 1; right < stored.length; right++) {
                            if (isKept[right]) {
                                BigDecimal dt = BigDecimal.valueOf(right - left);
                                BigDecimal anchor = new BigDecimal(stored[left]);
                                BigDecimal delta = new BigDecimal(stored[right]).subtract(anchor);
                                BigDecimal budget = BigDecimal.valueOf(7).multiply(dt);
                                for (int i = left + 1; i < right; i++) {
                                    BigDecimal error = new BigDecimal(stored[i]).subtract(anchor).multiply(dt)
                                            .subtract(delta.multiply(BigDecimal.valueOf(i - left))).abs();
                                    Assert.assertTrue(query + " exceeds 2 * compdev at row " + i, error.compareTo(budget) <= 0);
                                }
                                left = right;
                            }
                        }
                    }
                }
            }
        }
    }

    private void assertReconstructionBound(String timestampType) throws Exception {
        assertMemoryLeak(() -> {
            long[] bases = {0, 1L << 53, -(1L << 53)};
            int[] offsets = {0, 4, 8, 12, 20, 16};
            for (int table = 0; table < bases.length; table++) {
                String name = "t" + table;
                execute("CREATE TABLE " + name + " (ts " + timestampType + ", v DOUBLE) TIMESTAMP(ts)");
                StringBuilder insert = new StringBuilder("INSERT INTO " + name + " VALUES ");
                long base = bases[table];
                long sign = base < 0 ? -1 : 1;
                for (int i = 0; i < offsets.length; i++) {
                    if (i > 0) {
                        insert.append(',');
                    }
                    insert.append('(').append(i).append("::").append(timestampType).append(',')
                            .append(base + sign * offsets[i]).append("::DOUBLE)");
                }
                execute(insert);
                double[] stored = new double[offsets.length];
                try (RecordCursorFactory factory = select("SELECT ts, v FROM " + name);
                     RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Record record = cursor.getRecord();
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(count, record.getTimestamp(0));
                        stored[count] = record.getDouble(1);
                        Assert.assertEquals(BigDecimal.valueOf(base + sign * offsets[count]), new BigDecimal(stored[count]));
                        count++;
                    }
                    Assert.assertEquals(stored.length, count);
                }
                for (boolean isLight : new boolean[]{true, false}) {
                    setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, Boolean.toString(isLight));
                    String subsample = "SELECT ts, v FROM " + name + " SUBSAMPLE sdt(v, 3.5)";
                    String window = "SELECT ts, v, sdt(ts, v, 3.5) OVER (ORDER BY ts) AS keep FROM " + name;
                    assertQuery("SELECT ts::LONG AS t FROM (" + subsample + ")").returns("t\n0\n4\n5\n");
                    assertQuery("SELECT ts::LONG AS t, keep FROM (" + window + ")").expectSize().returns("""
                            t\tkeep
                            0\ttrue
                            1\tfalse
                            2\tfalse
                            3\tfalse
                            4\ttrue
                            5\ttrue
                            """);
                    assertExactError(subsample, stored, false);
                    assertExactError(window, stored, true);
                }
            }

            // The sorted window interleaves the positive and negative large-offset series.
            // Each partition reloads its certified corridor from the map between samples.
            execute("CREATE TABLE p (ts " + timestampType + ", v DOUBLE, k INT)");
            execute("INSERT INTO p SELECT ts, v, 0 FROM t1 UNION ALL SELECT ts, v, 1 FROM t2");
            for (boolean isLight : new boolean[]{true, false}) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, Boolean.toString(isLight));
                assertQuery("SELECT ts::LONG AS t, k, sdt(ts, v, 3.5) OVER (PARTITION BY k ORDER BY ts) AS keep FROM p ORDER BY t, k")
                        .expectSize()
                        .returns("""
                                t\tk\tkeep
                                0\t0\ttrue
                                0\t1\ttrue
                                1\t0\tfalse
                                1\t1\tfalse
                                2\t0\tfalse
                                2\t1\tfalse
                                3\t0\tfalse
                                3\t1\tfalse
                                4\t0\ttrue
                                4\t1\ttrue
                                5\t0\ttrue
                                5\t1\ttrue
                                """);
            }
        });
    }
}
