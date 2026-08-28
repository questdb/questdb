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
    // Fixed-width sort key columns. Wide fixed types (UUID, LONG256, LONG128) push the
    // multi-column key past 8 bytes and, when summed past 32 bytes, onto the variable path.
    private static final Column[] COLUMNS = {
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
            new Column("col_uuid", 16),
            new Column("col_long256", 32),
            new Column("col_long128", 16),
            new Column("ts", 8),
    };

    // Variable-length sort key columns. byteWidth is 0 so selectColumns ignores them in
    // the fixed-budget accounting; a query mixes them with fixed columns to force the
    // variable key path.
    private static final Column[] VAR_COLUMNS = {
            new Column("col_str", 0),
            new Column("col_str_long", 0),
            new Column("col_varchar", 0),
            new Column("col_varchar_long", 0),
            new Column("col_sym", 0),
    };

    @Override
    public void setUp() {
        node1.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_TOP_K_ENABLED, false);
        super.setUp();
    }

    @Test
    public void testFuzzEncodedSort() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = TestUtils.generateRandom(LOG);
            int rowCount = 50 + rnd.nextInt(4951);
            createFuzzTable("fuzz_sort", rowCount, rnd.nextBoolean());

            int[] targetMaxBytes = {8, 16, 24, 32, 40, 48, 64};

            // Light path
            for (int targetMax : targetMaxBytes) {
                for (int attempt = 0; attempt < 5; attempt++) {
                    ObjList<Column> selected = selectColumns(rnd, COLUMNS, targetMax);
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
                    ObjList<Column> selected = selectColumns(rnd, COLUMNS, targetMax);
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

            // Variable-length keys: random fixed columns mixed with string columns
            for (int attempt = 0; attempt < 10; attempt++) {
                ObjList<Column> selected = selectColumns(rnd, COLUMNS, 8 + rnd.nextInt(57));
                selected.add(VAR_COLUMNS[rnd.nextInt(VAR_COLUMNS.length)]);
                if (rnd.nextBoolean()) {
                    selected.add(VAR_COLUMNS[rnd.nextInt(VAR_COLUMNS.length)]);
                }
                shuffleColumns(rnd, selected);
                long parallelThreshold = rnd.nextLong(rowCount * 2L + 1);
                node1.setProperty(PropertyKey.CAIRO_SQL_SORT_ENCODED_PARALLEL_THRESHOLD, parallelThreshold);
                StringSink orderByClause = buildOrderByClause(rnd, selected);
                assertSortMatch(orderByClause, "SELECT * FROM fuzz_sort ORDER BY ", -1, "light variable");
            }

            for (int attempt = 0; attempt < 5; attempt++) {
                ObjList<Column> selected = selectColumns(rnd, COLUMNS, 8 + rnd.nextInt(57));
                selected.add(VAR_COLUMNS[rnd.nextInt(VAR_COLUMNS.length)]);
                shuffleColumns(rnd, selected);
                long parallelThreshold = rnd.nextLong(rowCount * 2L + 1);
                node1.setProperty(PropertyKey.CAIRO_SQL_SORT_ENCODED_PARALLEL_THRESHOLD, parallelThreshold);
                StringSink orderByClause = buildOrderByClause(rnd, selected);
                assertSortMatch(
                        orderByClause,
                        "SELECT * FROM (fuzz_sort UNION ALL SELECT * FROM fuzz_sort WHERE false) ORDER BY ",
                        -1,
                        "non-light variable"
                );
            }
        });
    }

    @Test
    public void testFuzzEncodedSortLimit() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = TestUtils.generateRandom(LOG);
            int rowCount = 50 + rnd.nextInt(4951);
            createFuzzTable("fuzz_sort_limit", rowCount, rnd.nextBoolean());

            Column[] nonTsColumns = new Column[COLUMNS.length - 1];
            System.arraycopy(COLUMNS, 0, nonTsColumns, 0, nonTsColumns.length);

            int[] targetMaxBytes = {16, 24, 32};
            for (int targetMax : targetMaxBytes) {
                for (int attempt = 0; attempt < 5; attempt++) {
                    ObjList<Column> selected = selectColumns(rnd, nonTsColumns, targetMax - 8);
                    if (selected.size() == 0) {
                        continue;
                    }
                    long parallelThreshold = rnd.nextLong(rowCount * 2L + 1);
                    node1.setProperty(PropertyKey.CAIRO_SQL_SORT_ENCODED_PARALLEL_THRESHOLD, parallelThreshold);
                    StringSink orderByClause = new StringSink();
                    if (rnd.nextBoolean()) {
                        orderByClause.put("ts, ").put(buildOrderByClause(rnd, selected));
                    } else {
                        orderByClause.put(buildOrderByClause(rnd, selected)).put(", ts");
                    }
                    StringSink query = new StringSink();
                    query.put("SELECT * FROM fuzz_sort_limit");
                    if (rnd.nextBoolean()) {
                        query.put(" WHERE col_long >= 3");
                    }
                    query.put(" ORDER BY ").put(orderByClause).put(' ');
                    appendLimitClause(rnd, query, rowCount);
                    assertQueryMatch(query, "limit");
                }
            }

            // Variable-length keys with LIMIT exercise the encoded top-K key heap.
            // ts trails the key so the order is total and the LIMIT cut is
            // deterministic across the encoded and legacy engines despite ties.
            for (int attempt = 0; attempt < 8; attempt++) {
                ObjList<Column> selected = selectColumns(rnd, nonTsColumns, rnd.nextInt(33));
                selected.add(VAR_COLUMNS[rnd.nextInt(VAR_COLUMNS.length)]);
                shuffleColumns(rnd, selected);
                long parallelThreshold = rnd.nextLong(rowCount * 2L + 1);
                node1.setProperty(PropertyKey.CAIRO_SQL_SORT_ENCODED_PARALLEL_THRESHOLD, parallelThreshold);
                StringSink query = new StringSink();
                query.put("SELECT * FROM fuzz_sort_limit ORDER BY ")
                        .put(buildOrderByClause(rnd, selected)).put(", ts ");
                appendLimitClause(rnd, query, rowCount);
                assertQueryMatch(query, "limit variable");
            }
        });
    }

    @Test
    public void testFuzzEncodedSortParquet() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = TestUtils.generateRandom(LOG);
            int rowCount = 50 + rnd.nextInt(4951);
            createFuzzTable("fuzz_sort_parquet", rowCount, true);

            int[] targetMaxBytes = {8, 16, 24, 32};
            for (int targetMax : targetMaxBytes) {
                for (int attempt = 0; attempt < 3; attempt++) {
                    ObjList<Column> selected = selectColumns(rnd, COLUMNS, targetMax);
                    if (selected.size() == 0) {
                        continue;
                    }
                    long parallelThreshold = rnd.nextLong(rowCount * 2L + 1);
                    node1.setProperty(PropertyKey.CAIRO_SQL_SORT_ENCODED_PARALLEL_THRESHOLD, parallelThreshold);
                    StringSink orderByClause = buildOrderByClause(rnd, selected);
                    assertSortMatch(orderByClause, "SELECT * FROM fuzz_sort_parquet ORDER BY ", targetMax, "parquet");

                    // The unique ts in the key makes the sort total, so the LIMIT cut
                    // cannot diverge between the engines on duplicate keys. Appending
                    // ts only fits the 32-byte encoder budget for the smaller keys.
                    boolean hasTs = false;
                    for (int i = 0, n = selected.size(); i < n; i++) {
                        hasTs |= "ts".equals(selected.getQuick(i).name());
                    }
                    if (hasTs || targetMax <= 24) {
                        StringSink query = new StringSink();
                        query.put("SELECT * FROM fuzz_sort_parquet ORDER BY ").put(orderByClause);
                        if (!hasTs) {
                            query.put(", ts");
                        }
                        query.put(' ');
                        appendLimitClause(rnd, query, rowCount);
                        assertQueryMatch(query, "parquet limit");
                    }
                }
            }
        });
    }

    @Test
    public void testFuzzEncodedSortParquetMultiRowGroup() throws Exception {
        // The default fuzz Parquet table is a single row group, so the row-filtered
        // emit always decodes from offset 0 of row group 0 and never reaches the
        // FILL_NULLS gap-fill path with a non-zero rowGroupLo. Force several row groups
        // per partition and start the scanned frame mid-row-group with a designated-
        // timestamp interval, so the emit declares rows past group 0 / past offset 0.
        // A unique ts in the ORDER BY keeps the sort total, so the LIMIT cut never
        // splits a tie group and the encoded and legacy results stay comparable.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 256);
        assertMemoryLeak(() -> {
            Rnd rnd = TestUtils.generateRandom(LOG);
            // Enough rows to span many row groups per hourly partition (256 rows each).
            int rowCount = 2_000 + rnd.nextInt(3_001);
            createFuzzTable("fuzz_sort_rg", rowCount, true);

            int[] targetMaxBytes = {8, 16, 24};
            for (int targetMax : targetMaxBytes) {
                for (int attempt = 0; attempt < 3; attempt++) {
                    ObjList<Column> selected = selectColumns(rnd, COLUMNS, targetMax);
                    if (selected.size() == 0) {
                        continue;
                    }
                    long parallelThreshold = rnd.nextLong(rowCount * 2L + 1);
                    node1.setProperty(PropertyKey.CAIRO_SQL_SORT_ENCODED_PARALLEL_THRESHOLD, parallelThreshold);
                    StringSink orderByClause = buildOrderByClause(rnd, selected);
                    boolean hasTs = false;
                    for (int i = 0, n = selected.size(); i < n; i++) {
                        hasTs |= "ts".equals(selected.getQuick(i).name());
                    }
                    StringSink query = new StringSink();
                    query.put("SELECT * FROM fuzz_sort_rg");
                    // ts = rowIndex * 1_000_000 micros, so an interval at a non-edge row
                    // starts the first scanned Parquet frame partway into a row group.
                    long startRow = 1 + rnd.nextInt(rowCount - 1);
                    query.put(" WHERE ts >= ").put(startRow * 1_000_000L).put("::timestamp");
                    query.put(" ORDER BY ").put(orderByClause);
                    if (!hasTs) {
                        query.put(", ts");
                    }
                    query.put(' ');
                    appendLimitClause(rnd, query, rowCount);
                    assertQueryMatch(query, "parquet multi-row-group limit");
                }
            }
        });
    }

    private static void appendLimitClause(Rnd rnd, StringSink query, int rowCount) {
        final int bound = rowCount + 10;
        switch (rnd.nextInt(6)) {
            case 0 -> query.put("LIMIT ").put(1 + rnd.nextInt(bound));
            case 1 -> query.put("LIMIT -").put(1 + rnd.nextInt(bound));
            case 2 -> {
                final int lo = rnd.nextInt(bound);
                query.put("LIMIT ").put(lo).put(',').put(lo + 1 + rnd.nextInt(bound));
            }
            case 3 -> query.put("LIMIT ").put(rnd.nextInt(bound)).put(",-").put(1 + rnd.nextInt(bound));
            // arbitrary positive pair: covers lo == hi (empty) and lo > hi (normalized)
            case 4 -> query.put("LIMIT ").put(rnd.nextInt(bound)).put(',').put(rnd.nextInt(bound));
            // arbitrary negative pair: covers lo == hi (empty) and reversed ranges
            default -> query.put("LIMIT -").put(1 + rnd.nextInt(bound)).put(",-").put(1 + rnd.nextInt(bound));
        }
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

    private static void shuffleColumns(Rnd rnd, ObjList<Column> columns) {
        for (int i = columns.size() - 1; i > 0; i--) {
            int j = rnd.nextInt(i + 1);
            Column tmp = columns.getQuick(i);
            columns.setQuick(i, columns.getQuick(j));
            columns.setQuick(j, tmp);
        }
    }

    private void assertQueryMatch(CharSequence query, String path) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, false);
        StringSink expected = new StringSink();
        printSql(query, expected);

        node1.setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, true);
        StringSink actual = new StringSink();
        printSql(query, actual);

        StringSink msg = new StringSink();
        msg.put(path).put(" mismatch for ").put(query);
        TestUtils.assertEquals(msg.toString(), expected, actual);
    }

    private void assertSortMatch(
            CharSequence orderByClause,
            String queryPrefix,
            int targetMax,
            String path
    ) throws Exception {
        StringSink query = new StringSink();
        query.put(queryPrefix).put(orderByClause);
        final String label = targetMax > 0 ? path + " (target key <= " + targetMax + " bytes)" : path;
        assertQueryMatch(query, label);
    }

    private void createFuzzTable(String tableName, int rowCount, boolean isParquet) throws Exception {
        execute(
                "CREATE TABLE " + tableName + " AS (SELECT" +
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
                        " rnd_uuid4() col_uuid," +
                        " rnd_long256() col_long256," +
                        " to_long128(rnd_long(), rnd_long()) col_long128," +
                        " rnd_str(1, 24, 2) col_str," +
                        " rnd_str(16, 64, 2) col_str_long," +
                        " rnd_varchar(1, 24, 2) col_varchar," +
                        " rnd_varchar(16, 64, 2) col_varchar_long," +
                        " rnd_double_array(2) col_arr," +
                        " timestamp_sequence(0, 1_000_000) ts" +
                        " FROM long_sequence(" + rowCount + ")) TIMESTAMP(ts)" +
                        (isParquet ? " PARTITION BY HOUR" : "")
        );
        if (isParquet) {
            // A row in a later partition makes the generated partitions convertible.
            execute("INSERT INTO " + tableName + "(ts) VALUES ('2000-01-01')");
            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET WHERE ts < '2000-01-01'");
        }
    }

    private record Column(String name, int byteWidth) {
    }
}
