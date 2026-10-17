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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * A calendar SAMPLE BY grid with TIME ZONE starts each bucket at the first instant the local wall clock
 * reads the bucket's grid line or later, and every execution path floors to that grid.
 */
public class SampleByDstGridTest extends AbstractCairoTest {
    private static final String[] WINDOWS = {
            "2026-03-06T18:00", "2026-03-27T12:00", "2026-04-03T06:00", "2026-10-02T06:00", "2026-10-23T12:00", "2026-10-30T18:00"
    };

    @Test
    public void testOffsetInsideTransitionWindow() throws Exception {
        final String[][] cases = {
                {"Europe/Berlin", "02:30", "1d", """
                    ts\tc\ts
                    2026-03-06T01:30:00.000000Z\t15\t120
                    2026-03-07T01:30:00.000000Z\t48\t1896
                    2026-03-08T01:30:00.000000Z\t48\t4200
                    2026-03-09T01:30:00.000000Z\t9\t1044
                    2026-03-27T01:30:00.000000Z\t27\t3618
                    2026-03-28T01:30:00.000000Z\t47\t8037
                    2026-03-29T01:00:00.000000Z\t46\t10005
                    2026-04-03T00:30:00.000000Z\t37\t9583
                    2026-04-04T00:30:00.000000Z\t48\t14472
                    2026-04-05T00:30:00.000000Z\t35\t12005
                    2026-10-02T00:30:00.000000Z\t37\t14023
                    2026-10-03T00:30:00.000000Z\t48\t20232
                    2026-10-04T00:30:00.000000Z\t35\t16205
                    2026-10-23T00:30:00.000000Z\t25\t12325
                    2026-10-24T00:30:00.000000Z\t48\t25416
                    2026-10-25T00:30:00.000000Z\t47\t27119
                    2026-10-30T01:30:00.000000Z\t15\t9120
                    2026-10-31T01:30:00.000000Z\t48\t30696
                    2026-11-01T01:30:00.000000Z\t48\t33000
                    2026-11-02T01:30:00.000000Z\t9\t6444
                    """},
                {"Europe/Berlin", "02:00", "1d", """
                    ts\tc\ts
                    2026-03-06T01:00:00.000000Z\t14\t105
                    2026-03-07T01:00:00.000000Z\t48\t1848
                    2026-03-08T01:00:00.000000Z\t48\t4152
                    2026-03-09T01:00:00.000000Z\t10\t1155
                    2026-03-27T01:00:00.000000Z\t26\t3471
                    2026-03-28T01:00:00.000000Z\t48\t8184
                    2026-03-29T01:00:00.000000Z\t46\t10005
                    2026-04-03T00:00:00.000000Z\t36\t9306
                    2026-04-04T00:00:00.000000Z\t48\t14424
                    2026-04-05T00:00:00.000000Z\t36\t12330
                    2026-10-02T00:00:00.000000Z\t36\t13626
                    2026-10-03T00:00:00.000000Z\t48\t20184
                    2026-10-04T00:00:00.000000Z\t36\t16650
                    2026-10-23T00:00:00.000000Z\t24\t11820
                    2026-10-24T00:00:00.000000Z\t48\t25368
                    2026-10-25T00:00:00.000000Z\t48\t27672
                    2026-10-30T01:00:00.000000Z\t14\t8505
                    2026-10-31T01:00:00.000000Z\t48\t30648
                    2026-11-01T01:00:00.000000Z\t48\t32952
                    2026-11-02T01:00:00.000000Z\t10\t7155
                    """},
                {"America/New_York", "01:30", "1d", """
                    ts\tc\ts
                    2026-03-06T06:30:00.000000Z\t25\t325
                    2026-03-07T06:30:00.000000Z\t48\t2376
                    2026-03-08T06:30:00.000000Z\t46\t4439
                    2026-03-09T05:30:00.000000Z\t1\t120
                    2026-03-27T05:30:00.000000Z\t35\t4830
                    2026-03-28T05:30:00.000000Z\t48\t8616
                    2026-03-29T05:30:00.000000Z\t37\t8214
                    2026-04-03T05:30:00.000000Z\t47\t12408
                    2026-04-04T05:30:00.000000Z\t48\t14952
                    2026-04-05T05:30:00.000000Z\t25\t8700
                    2026-10-02T05:30:00.000000Z\t47\t18048
                    2026-10-03T05:30:00.000000Z\t48\t20712
                    2026-10-04T05:30:00.000000Z\t25\t11700
                    2026-10-23T05:30:00.000000Z\t35\t17430
                    2026-10-24T05:30:00.000000Z\t48\t25896
                    2026-10-25T05:30:00.000000Z\t37\t21534
                    2026-10-30T05:30:00.000000Z\t23\t14076
                    2026-10-31T05:30:00.000000Z\t48\t31080
                    2026-11-01T05:30:00.000000Z\t49\t34104
                    """},
                {"America/New_York", "02:30", "1d", """
                    ts\tc\ts
                    2026-03-06T07:30:00.000000Z\t27\t378
                    2026-03-07T07:30:00.000000Z\t47\t2397
                    2026-03-08T07:00:00.000000Z\t46\t4485
                    2026-03-27T06:30:00.000000Z\t37\t5143
                    2026-03-28T06:30:00.000000Z\t48\t8712
                    2026-03-29T06:30:00.000000Z\t35\t7805
                    2026-04-02T06:30:00.000000Z\t1\t241
                    2026-04-03T06:30:00.000000Z\t48\t12744
                    2026-04-04T06:30:00.000000Z\t48\t15048
                    2026-04-05T06:30:00.000000Z\t23\t8027
                    2026-10-01T06:30:00.000000Z\t1\t361
                    2026-10-02T06:30:00.000000Z\t48\t18504
                    2026-10-03T06:30:00.000000Z\t48\t20808
                    2026-10-04T06:30:00.000000Z\t23\t10787
                    2026-10-23T06:30:00.000000Z\t37\t18463
                    2026-10-24T06:30:00.000000Z\t48\t25992
                    2026-10-25T06:30:00.000000Z\t35\t20405
                    2026-10-30T06:30:00.000000Z\t25\t15325
                    2026-10-31T06:30:00.000000Z\t50\t32525
                    2026-11-01T07:30:00.000000Z\t45\t31410
                    """},
                {"Australia/Sydney", "02:30", "1d", """
                    ts\tc\ts
                    2026-03-06T15:30:00.000000Z\t43\t946
                    2026-03-07T15:30:00.000000Z\t48\t3240
                    2026-03-08T15:30:00.000000Z\t29\t3074
                    2026-03-26T15:30:00.000000Z\t7\t868
                    2026-03-27T15:30:00.000000Z\t48\t7272
                    2026-03-28T15:30:00.000000Z\t48\t9576
                    2026-03-29T15:30:00.000000Z\t17\t3944
                    2026-04-02T15:30:00.000000Z\t19\t4750
                    2026-04-03T15:30:00.000000Z\t48\t13608
                    2026-04-04T15:30:00.000000Z\t50\t16625
                    2026-04-05T16:30:00.000000Z\t3\t1077
                    2026-10-01T16:30:00.000000Z\t21\t7791
                    2026-10-02T16:30:00.000000Z\t47\t19035
                    2026-10-03T16:00:00.000000Z\t47\t21244
                    2026-10-04T15:30:00.000000Z\t5\t2390
                    2026-10-22T15:30:00.000000Z\t7\t3388
                    2026-10-23T15:30:00.000000Z\t48\t24552
                    2026-10-24T15:30:00.000000Z\t48\t26856
                    2026-10-25T15:30:00.000000Z\t17\t10064
                    2026-10-30T15:30:00.000000Z\t43\t26746
                    2026-10-31T15:30:00.000000Z\t48\t32040
                    2026-11-01T15:30:00.000000Z\t29\t20474
                    """},
                {"Europe/Berlin", "02:30", "2d", """
                    ts\tc\ts
                    2026-03-06T01:30:00.000000Z\t63\t2016
                    2026-03-08T01:30:00.000000Z\t57\t5244
                    2026-03-26T01:30:00.000000Z\t27\t3618
                    2026-03-28T01:30:00.000000Z\t93\t18042
                    2026-04-03T00:30:00.000000Z\t85\t24055
                    2026-04-05T00:30:00.000000Z\t35\t12005
                    2026-10-02T00:30:00.000000Z\t85\t34255
                    2026-10-04T00:30:00.000000Z\t35\t16205
                    2026-10-22T00:30:00.000000Z\t25\t12325
                    2026-10-24T00:30:00.000000Z\t95\t52535
                    2026-10-30T01:30:00.000000Z\t63\t39816
                    2026-11-01T01:30:00.000000Z\t57\t39444
                    """},
                {"Europe/Berlin", "02:30", "1M", """
                    ts\tc\ts
                    2026-03-01T01:30:00.000000Z\t240\t28920
                    2026-04-01T00:30:00.000000Z\t120\t36060
                    2026-10-01T00:30:00.000000Z\t303\t155136
                    2026-11-01T01:30:00.000000Z\t57\t39444
                    """},
        };
        assertMemoryLeak(() -> {
            for (String type : new String[]{"TIMESTAMP", "TIMESTAMP_NS"}) {
                createTable(type);
                for (String[] c : cases) {
                    final String zone = c[0];
                    final String offset = c[1];
                    final String stride = c[2];
                    final String grid = " ALIGN TO CALENDAR TIME ZONE '" + zone + "' WITH OFFSET '" + offset + "'";
                    final String expected = "TIMESTAMP_NS".equals(type) ? c[3].replace(".000000Z", ".000000000Z") : c[3];
                    assertQuery("SELECT ts, count() c, sum(x) s FROM gx SAMPLE BY " + stride + grid)
                            .noLeakCheck()
                            .timestamp("ts")
                            .inferRandomAccess()
                            .sizeMayVary()
                            .returns(expected);
                    assertQuery("SELECT timestamp_floor_utc('" + stride + "', ts, null, '" + offset + "', '" + zone + "') ts, count() c, sum(x) s FROM gx ORDER BY ts")
                            .noLeakCheck()
                            .inferTimestamp()
                            .inferRandomAccess()
                            .sizeMayVary()
                            .returns(expected);
                    assertQuery("SELECT c, s FROM (SELECT k, count() c, sum(x) s FROM (SELECT timestamp_floor('" + stride + "', ts, null, '" + offset + "', '" + zone + "') k, x FROM gx) ORDER BY k)")
                            .noLeakCheck()
                            .inferRandomAccess()
                            .sizeMayVary()
                            .returns(expected.replaceAll("(?m)^[^\\t\\n]*\\t", ""));
                    assertSourcesAgree(stride, grid);
                }
                for (String zone : new String[]{"Europe/Berlin", "America/New_York", "Australia/Sydney"}) {
                    for (String offset : new String[]{"01:30", "02:00", "02:30"}) {
                        final String grid = " ALIGN TO CALENDAR TIME ZONE '" + zone + "'";
                        for (String stride : new String[]{"1d", "2d", "1M"}) {
                            assertSourcesAgree(stride, grid + " WITH OFFSET '" + offset + "'");
                            printSql("SELECT ts, min(x) f, max(x) l FROM gx WHERE sym = 'a' SAMPLE BY " + stride + grid + " WITH OFFSET '" + offset + "'");
                            assertQuery("SELECT ts, first(x) f, last(x) l FROM gx WHERE sym = 'a' SAMPLE BY " + stride
                                    + " FROM dateadd('d', 0, '2026-01-01T" + offset + "'::TIMESTAMP)" + grid)
                                    .noLeakCheck()
                                    .timestamp("ts")
                                    .inferRandomAccess()
                                    .sizeMayVary()
                                    .withPlanContaining("SampleByFirstLast")
                                    .returns(sink.toString());
                        }
                    }
                }
                execute("DROP TABLE gx");
            }
        });
    }

    private static void createTable(String type) throws Exception {
        execute("CREATE TABLE gx (sym SYMBOL INDEX, x LONG, ts " + type + ") TIMESTAMP(ts) PARTITION BY DAY");
        for (int i = 0; i < WINDOWS.length; i++) {
            execute("INSERT INTO gx SELECT CASE WHEN x % 2 = 0 THEN 'b' ELSE 'a' END, x + " + i * 120
                    + ", timestamp_sequence('" + WINDOWS[i] + "', 1_800_000_000L) FROM long_sequence(120)");
        }
    }

    // Every source spelling and FILL mode buckets like the table without FILL; LINEAR fills the same buckets.
    private void assertSourcesAgree(String stride, String grid) throws Exception {
        final String[] fills = {"", " FILL(NULL)", " FILL(PREV)", " FILL(0)", " FILL(LINEAR)"};
        for (String keys : new String[]{"", "sym, "}) {
            final String order = keys.isEmpty() ? "" : " ORDER BY ts, sym";
            for (String fill : fills) {
                final String projection = "SELECT ts, " + keys + "count() c, sum(x) s FROM ";
                final String tail = " SAMPLE BY " + stride + fill + grid + ")" + order;
                printSql("SELECT * FROM (" + projection + "gx" + tail);
                final String expected = sink.toString();
                for (String sql : new String[]{
                        "SELECT * FROM (" + projection + "(SELECT * FROM gx WHERE x > 0) TIMESTAMP(ts)" + tail,
                        "WITH q AS (SELECT * FROM gx WHERE x > 0) SELECT * FROM (" + projection + "q" + tail
                }) {
                    assertQuery(sql).noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);
                }
                if (keys.isEmpty() && fill.equals(" FILL(NULL)")) {
                    printSql("SELECT * FROM (" + projection + "gx SAMPLE BY " + stride + " FILL(LINEAR)" + grid + ")");
                    Assert.assertEquals(grid, expected.replaceAll("\\t[^\\n]*", ""), sink.toString().replaceAll("\\t[^\\n]*", ""));
                }
            }
        }
    }
}
