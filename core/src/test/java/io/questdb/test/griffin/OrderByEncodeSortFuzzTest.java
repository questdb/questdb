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

import io.questdb.PropertyKey;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class OrderByEncodeSortFuzzTest extends AbstractCairoTest {

    @Test
    public void testFuzzEncodedSort() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = TestUtils.generateRandom(LOG);
            int rowCount = 50 + rnd.nextInt(4951);

            execute(
                    "CREATE TABLE fuzz_sort AS (SELECT" +
                            " rnd_boolean() col_bool," +
                            " rnd_byte() col_byte," +
                            " rnd_short() col_short," +
                            " rnd_char() col_char," +
                            " rnd_int(0, 10, 2) col_int," +
                            " rnd_float(2) col_float," +
                            " rnd_symbol(4, 2, 4, 0) col_sym," +
                            " rnd_symbol(4, 2, 4, 2) col_sym_null," +
                            " rnd_ipv4() col_ipv4," +
                            " rnd_long(0, 10, 2) col_long," +
                            " rnd_double(2) col_double," +
                            " rnd_date(0, 100_000_000_000L, 2) col_date," +
                            " rnd_geohash(5) col_geobyte," +
                            " rnd_geohash(10) col_geoshort," +
                            " rnd_geohash(20) col_geoint," +
                            " rnd_geohash(40) col_geolong," +
                            " rnd_decimal(2, 1, 2) col_dec8," +
                            " rnd_decimal(4, 2, 2) col_dec16," +
                            " rnd_decimal(9, 3, 2) col_dec32," +
                            " rnd_decimal(18, 4, 2) col_dec64," +
                            " rnd_decimal(38, 5, 2) col_dec128," +
                            " rnd_decimal(76, 6, 2) col_dec256," +
                            " timestamp_sequence(0, 1_000_000) ts" +
                            " FROM long_sequence(" + rowCount + ")) TIMESTAMP(ts)"
            );

            Column[] columns = {
                    new Column("col_bool", 1),
                    new Column("col_byte", 1),
                    new Column("col_short", 2),
                    new Column("col_char", 2),
                    new Column("col_int", 4),
                    new Column("col_float", 4),
                    new Column("col_sym", 1),
                    new Column("col_sym_null", 1),
                    new Column("col_ipv4", 4),
                    new Column("col_long", 8),
                    new Column("col_double", 8),
                    new Column("col_date", 8),
                    new Column("col_geobyte", 1),
                    new Column("col_geoshort", 2),
                    new Column("col_geoint", 4),
                    new Column("col_geolong", 8),
                    new Column("col_dec8", 1),
                    new Column("col_dec16", 2),
                    new Column("col_dec32", 4),
                    new Column("col_dec64", 8),
                    new Column("col_dec128", 16),
                    new Column("col_dec256", 32),
                    new Column("ts", 8),
            };

            int[] targetMaxBytes = {8, 16, 24, 32};

            // Light path
            for (int targetMax : targetMaxBytes) {
                for (int attempt = 0; attempt < 5; attempt++) {
                    ObjList<Column> selected = selectColumns(rnd, columns, targetMax);
                    if (selected.size() == 0) {
                        continue;
                    }
                    long parallelThreshold = rnd.nextLong(rowCount * 2L + 1);
                    node1.setProperty(PropertyKey.CAIRO_SQL_SORT_ENCODED_PARALLEL_THRESHOLD, parallelThreshold);
                    StringSink orderByClause = buildOrderByClause(rnd, selected);
                    assertSortMatch(orderByClause, "SELECT * FROM fuzz_sort ORDER BY ", targetMax, "light");
                }
            }

            // Non-light path
            for (int targetMax : targetMaxBytes) {
                for (int attempt = 0; attempt < 3; attempt++) {
                    ObjList<Column> selected = selectColumns(rnd, columns, targetMax);
                    if (selected.size() == 0) {
                        continue;
                    }
                    long parallelThreshold = rnd.nextLong(rowCount * 2L + 1);
                    node1.setProperty(PropertyKey.CAIRO_SQL_SORT_ENCODED_PARALLEL_THRESHOLD, parallelThreshold);
                    StringSink orderByClause = buildOrderByClause(rnd, selected);
                    assertSortMatch(
                            orderByClause,
                            "SELECT * FROM (fuzz_sort UNION ALL SELECT * FROM fuzz_sort WHERE false) ORDER BY ",
                            targetMax,
                            "non-light"
                    );
                }
            }
        });
    }

    private static StringSink buildOrderByClause(Rnd rnd, ObjList<Column> columns) {
        StringSink sb = new StringSink();
        for (int i = 0, n = columns.size(); i < n; i++) {
            if (i > 0) {
                sb.put(", ");
            }
            sb.put(columns.getQuick(i).name);
            if (rnd.nextBoolean()) {
                sb.put(" DESC");
            }
        }
        return sb;
    }

    private static ObjList<Column> selectColumns(Rnd rnd, Column[] columns, int targetMax) {
        int targetMin = targetMax - 7;
        ObjList<Column> selected = new ObjList<>();
        int totalBytes = 0;

        int[] indices = new int[columns.length];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = i;
        }
        for (int i = indices.length - 1; i > 0; i--) {
            int j = rnd.nextInt(i + 1);
            int tmp = indices[i];
            indices[i] = indices[j];
            indices[j] = tmp;
        }

        for (int idx : indices) {
            Column col = columns[idx];
            if (totalBytes + col.byteWidth <= targetMax) {
                selected.add(col);
                totalBytes += col.byteWidth;
            }
            if (totalBytes >= targetMin && totalBytes <= targetMax) {
                if (rnd.nextInt(3) == 0) {
                    break;
                }
            }
        }

        if (selected.size() == 0) {
            for (Column col : columns) {
                if (col.byteWidth <= targetMax) {
                    selected.add(col);
                    break;
                }
            }
        }

        return selected;
    }

    private void assertSortMatch(
            CharSequence orderByClause,
            String queryPrefix,
            int targetMax,
            String path
    ) throws Exception {
        StringSink query = new StringSink();
        query.put(queryPrefix).put(orderByClause);

        node1.setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, false);
        StringSink expected = new StringSink();
        printSql(query, expected);

        node1.setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, true);
        StringSink actual = new StringSink();
        printSql(query, actual);

        StringSink msg = new StringSink();
        msg.put(path).put(" mismatch for ORDER BY ").put(orderByClause)
                .put(" (target key <= ").put(targetMax).put(" bytes)");
        TestUtils.assertEquals(msg.toString(), expected, actual);
    }

    private record Column(String name, int byteWidth) {
    }
}
