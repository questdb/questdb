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

package io.questdb.test.cairo.parquet;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Extra coverage for the column_top + NOT NULL Parquet round-trip path. The
 * existing test {@code NotNullColumnTest#testNotNullParquetRoundTripWithColumnTop}
 * pins the LONG-only single-column case; this test adds:
 * <ul>
 *   <li>Multiple NOT NULL columns added at different times so the partition
 *       has a staircase of column_top boundaries within a single
 *       CONVERT TO PARQUET execution.</li>
 *   <li>Round-trip back to native after the parquet conversion to verify the
 *       Rust reader path (parquet_read) also preserves the column_top sentinel
 *       layout.</li>
 *   <li>Multiple post-column_top rows so the
 *       {@code encode_not_null_def_levels} loop iterates rather than
 *       degenerating to a single element.</li>
 * </ul>
 * The production code under test is in
 * {@code core/rust/qdbr/src/parquet_write/primitive.rs} —
 * {@code encode_not_null_def_levels} branches on {@code column_top > 0}.
 */
public class ParquetNotNullColumnTopTest extends AbstractCairoTest {

    @Test
    public void testParquetRoundTripWithColumnTopOnNotNull() throws Exception {
        // Three-stage construction so the parquet writer must encode two
        // distinct column_top boundaries on top of the all-rows-present
        // designated timestamp:
        //   - rows 0..2  (3 rows): only ts + a present; b and c are below their column_top.
        //   - rows 3..5  (3 rows): ts + a + b present; c still below its column_top.
        //   - rows 6..8  (3 rows): ts + a + b + c all present.
        //
        // After CONVERT TO PARQUET the b column has column_top = 3 and the c
        // column has column_top = 6. The Rust writer must emit the correct
        // definition-level prefix for each, and the read-back must surface the
        // type's null sentinel for the rows below column_top while preserving
        // the actual value for the post-boundary rows.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (ts TIMESTAMP NOT NULL, a INT NOT NULL)
                    TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t VALUES
                        ('2024-06-10T00:00:00', 1),
                        ('2024-06-10T00:00:01', 2),
                        ('2024-06-10T00:00:02', 3)
                    """);

            // First boundary: b is added after 3 rows already exist.
            execute("ALTER TABLE t ADD COLUMN b INT NOT NULL");
            execute("""
                    INSERT INTO t VALUES
                        ('2024-06-10T00:00:03', 4, 40),
                        ('2024-06-10T00:00:04', 5, 50),
                        ('2024-06-10T00:00:05', 6, 60)
                    """);

            // Second boundary: c is added after 6 rows exist.
            execute("ALTER TABLE t ADD COLUMN c LONG NOT NULL");
            execute("""
                    INSERT INTO t VALUES
                        ('2024-06-10T00:00:06', 7, 70, 700),
                        ('2024-06-10T00:00:07', 8, 80, 800),
                        ('2024-06-10T00:00:08', 9, 90, 900)
                    """);

            // Convert the partition to parquet — exercises encode_not_null_def_levels
            // with column_top > 0 for both b (column_top = 3) and c (column_top = 6).
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-06-10'");

            // Read-back from parquet. The rows below column_top must surface the
            // type's null sentinel (the NOT NULL CursorPrinter branch renders raw
            // bit patterns, so INT_NULL is -2147483648 and LONG_NULL is
            // -9223372036854775808).
            assertSql(
                    """
                            ts\ta\tb\tc
                            2024-06-10T00:00:00.000000Z\t1\t-2147483648\t-9223372036854775808
                            2024-06-10T00:00:01.000000Z\t2\t-2147483648\t-9223372036854775808
                            2024-06-10T00:00:02.000000Z\t3\t-2147483648\t-9223372036854775808
                            2024-06-10T00:00:03.000000Z\t4\t40\t-9223372036854775808
                            2024-06-10T00:00:04.000000Z\t5\t50\t-9223372036854775808
                            2024-06-10T00:00:05.000000Z\t6\t60\t-9223372036854775808
                            2024-06-10T00:00:06.000000Z\t7\t70\t700
                            2024-06-10T00:00:07.000000Z\t8\t80\t800
                            2024-06-10T00:00:08.000000Z\t9\t90\t900
                            """,
                    "SELECT * FROM t ORDER BY ts"
            );

            // NOT NULL flag must survive in the parquet partition's metadata.
            try (TableReader reader = engine.getReader("t")) {
                TableReaderMetadata md = reader.getMetadata();
                assertTrue(md.isNotNull(md.getColumnIndex("a")));
                assertTrue(md.isNotNull(md.getColumnIndex("b")));
                assertTrue(md.isNotNull(md.getColumnIndex("c")));
                assertTrue(md.isNotNull(md.getColumnIndex("ts")));
            }

            // Convert back to native — exercises the Rust reader path
            // (parquet_read/row_groups.rs NOT NULL plumbing) and must
            // reconstruct the same column_top sentinel layout.
            execute("ALTER TABLE t CONVERT PARTITION TO NATIVE LIST '2024-06-10'");

            assertSql(
                    """
                            ts\ta\tb\tc
                            2024-06-10T00:00:00.000000Z\t1\t-2147483648\t-9223372036854775808
                            2024-06-10T00:00:01.000000Z\t2\t-2147483648\t-9223372036854775808
                            2024-06-10T00:00:02.000000Z\t3\t-2147483648\t-9223372036854775808
                            2024-06-10T00:00:03.000000Z\t4\t40\t-9223372036854775808
                            2024-06-10T00:00:04.000000Z\t5\t50\t-9223372036854775808
                            2024-06-10T00:00:05.000000Z\t6\t60\t-9223372036854775808
                            2024-06-10T00:00:06.000000Z\t7\t70\t700
                            2024-06-10T00:00:07.000000Z\t8\t80\t800
                            2024-06-10T00:00:08.000000Z\t9\t90\t900
                            """,
                    "SELECT * FROM t ORDER BY ts"
            );

            try (TableReader reader = engine.getReader("t")) {
                TableReaderMetadata md = reader.getMetadata();
                assertTrue(md.isNotNull(md.getColumnIndex("a")));
                assertTrue(md.isNotNull(md.getColumnIndex("b")));
                assertTrue(md.isNotNull(md.getColumnIndex("c")));
            }
        });
    }
}
