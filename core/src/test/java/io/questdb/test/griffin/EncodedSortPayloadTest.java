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

import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * A materialized encoded sort of a key and at most one small column sorts its entries alone (the key and
 * that column's value, EncodedSortPayloadRecordCursor): the rows, and their order among equal keys, are those
 * of the record chain sort of the same rows with one more column.
 */
public class EncodedSortPayloadTest extends AbstractCairoTest {

    @Test
    public void testPayloadSortMatchesTheChainSort() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT"
                    + " timestamp_sequence('2024-01-01', 1000) ts,"
                    + " CAST(CASE WHEN x % 41 = 0 THEN NULL ELSE rnd_int(-300, 300, 0) / 4.0 END AS FLOAT) f,"
                    + " CASE WHEN x % 43 = 0 THEN NULL ELSE rnd_int(-200, 200, 0) / 8.0 END d,"
                    + " rnd_int(-50, 50, 3) i,"
                    + " rnd_long(-1000, 1000, 3) l,"
                    + " dateadd('s', rnd_int(0, 300, 0), '2024-01-01'::TIMESTAMP) t2,"
                    + " rnd_boolean() b, rnd_short() sh, rnd_byte() by, rnd_char() ch"
                    + " FROM long_sequence(30_000)) TIMESTAMP(ts) PARTITION BY HOUR");
            // a UNION ALL base has no random access: the sort is a materialized one
            final String[] keys = {"f", "d", "i", "l", "t2", "(f * 2)"};
            final String[] payloads = {"ts", "i", "f", "d", "b", "sh", "by", "ch", null};
            for (String k : keys) {
                for (String p : payloads) {
                    for (String dir : new String[]{"", " DESC"}) {
                        final String cols = p == null ? "k" : "p, k";
                        final String sel = p == null ? k + " k" : p + " p, " + k + " k";
                        // the sort's rows are (p, k) alone: the payload sort
                        final String sql = "SELECT " + cols + " FROM (SELECT " + sel + " FROM t UNION ALL SELECT " + sel + " FROM t WHERE 1 = 0) ORDER BY k" + dir;
                        // one more column, which the query returns so that it stays: the record chain sort
                        final String reference = "SELECT " + cols + ", z FROM (SELECT " + sel + ", ts z FROM t UNION ALL SELECT " + sel + ", ts z FROM t WHERE 1 = 0) ORDER BY k" + dir;
                        final StringSink plan = new StringSink();
                        printSql("EXPLAIN " + sql, plan);
                        TestUtils.assertContains(plan, "Encode sort\n");
                        final StringSink a = new StringSink();
                        final StringSink b = new StringSink();
                        printSql(sql, a);
                        printSql(reference, b);
                        TestUtils.assertEquals(sql, withoutLastColumn(b), a);
                    }
                }
            }
        });
    }

    private static String withoutLastColumn(CharSequence text) {
        final StringBuilder sb = new StringBuilder();
        for (String line : text.toString().split("\n")) {
            sb.append(line, 0, line.lastIndexOf('\t')).append('\n');
        }
        return sb.toString();
    }
}
