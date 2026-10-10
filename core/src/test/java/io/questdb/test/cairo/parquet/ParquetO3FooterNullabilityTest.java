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

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.griffin.engine.table.parquet.ParquetFileDecoder;
import io.questdb.std.MemoryTag;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * O3 commits into a Parquet partition must synchronise the footer's QuestDB
 * nullability metadata ({@code QdbMetaCol.not_null}) with the table's CURRENT
 * logical schema, even when the ALTER was nullability-only and therefore
 * invisible to the physical-shape schema-change detection in
 * {@code O3PartitionJob} (which gates {@code setTargetSchema}). Without the
 * sync, {@code ParquetUpdater.end()} in
 * {@code core/rust/qdbr/src/parquet_write/update.rs} carries the STALE flag
 * from the old footer forward, and a file that physically contains a NULL
 * claims NOT NULL to every footer consumer (e.g. {@code read_parquet}).
 * <p>
 * The footer rule pinned here, in both directions:
 * <ul>
 *   <li>{@code SET NULL} then O3: the footer flag must drop to nullable.</li>
 *   <li>{@code SET NOT NULL} then O3: the footer flag must rise to not-null.
 *       This is truthful without a rewrite because the parquet Repetition
 *       stays Optional and the pages' definition levels and null-count
 *       statistics are untouched; physically-present old NULL entries decode
 *       to the type's sentinel bit pattern, which under the new logical
 *       schema is data — exactly the in-place reclassification contract of
 *       {@code ALTER COLUMN ... SET NOT NULL}.</li>
 * </ul>
 */
public class ParquetO3FooterNullabilityTest extends AbstractCairoTest {

    @Test
    public void testDropNotNullO3SingleRowGroupRewriteSyncsFooter() throws Exception {
        // A single-row-group file always takes the REWRITE path in
        // O3PartitionJob. A nullability-only ALTER does not set a target
        // schema, so the rewrite reuses the old footer's QdbMeta — the same
        // stale-flag hazard as update mode, through the other end() branch.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (x INT NOT NULL, ts TIMESTAMP NOT NULL)
                    TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t VALUES
                        (1, '2020-01-01T01:00:00.000Z'),
                        (2, '2020-01-01T02:00:00.000Z'),
                        (3, '2020-01-01T03:00:00.000Z'),
                        (1000, '2020-01-02T00:00:00.000Z')
                    """);
            drainWalQueue();

            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            Assert.assertTrue("footer must report NOT NULL after conversion", footerNotNull("t", "x"));

            execute("ALTER TABLE t ALTER COLUMN x SET NULL");
            drainWalQueue();
            execute("INSERT INTO t VALUES (NULL, '2020-01-01T00:30:00.000Z')");
            drainWalQueue();

            assertQuery("SELECT count() c FROM t WHERE x IS NULL")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            c
                            1
                            """);
            Assert.assertFalse(
                    "footer must report nullable after SET NULL + O3 rewrite",
                    footerNotNull("t", "x")
            );
        });
    }

    @Test
    public void testDropNotNullO3UpdateSyncsFooter() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        // Keep the O3 in UPDATE mode: no rewrite triggers.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, 1_000_000_000);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (x INT NOT NULL, ts TIMESTAMP NOT NULL)
                    TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            // 8 rows -> 2 row groups of 4; extra row keeps 2020-01-01 non-active.
            execute("""
                    INSERT INTO t VALUES
                        (1, '2020-01-01T01:00:00.000Z'),
                        (2, '2020-01-01T02:00:00.000Z'),
                        (3, '2020-01-01T03:00:00.000Z'),
                        (4, '2020-01-01T04:00:00.000Z'),
                        (5, '2020-01-01T05:00:00.000Z'),
                        (6, '2020-01-01T06:00:00.000Z'),
                        (7, '2020-01-01T07:00:00.000Z'),
                        (8, '2020-01-01T08:00:00.000Z'),
                        (1000, '2020-01-02T00:00:00.000Z')
                    """);
            drainWalQueue();

            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            Assert.assertTrue("footer must report NOT NULL after conversion", footerNotNull("t", "x"));

            // Nullability-only ALTER: no physical schema change, so the O3
            // update below bypasses setTargetSchema.
            execute("ALTER TABLE t ALTER COLUMN x SET NULL");
            drainWalQueue();

            // O3-insert a genuine NULL before every existing row group.
            execute("INSERT INTO t VALUES (NULL, '2020-01-01T00:30:00.000Z')");
            drainWalQueue();

            // Table-level truth: the partition physically holds one NULL.
            assertQuery("SELECT count() c FROM t WHERE x IS NULL")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            c
                            1
                            """);

            // The footer must carry the current logical schema.
            Assert.assertFalse(
                    "footer must report nullable after SET NULL + O3 update",
                    footerNotNull("t", "x")
            );
        });
    }

    @Test
    public void testNoAlterO3UpdateKeepsFooterNotNull() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, 1_000_000_000);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (x INT NOT NULL, ts TIMESTAMP NOT NULL)
                    TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t VALUES
                        (1, '2020-01-01T01:00:00.000Z'),
                        (2, '2020-01-01T02:00:00.000Z'),
                        (3, '2020-01-01T03:00:00.000Z'),
                        (4, '2020-01-01T04:00:00.000Z'),
                        (5, '2020-01-01T05:00:00.000Z'),
                        (6, '2020-01-01T06:00:00.000Z'),
                        (7, '2020-01-01T07:00:00.000Z'),
                        (8, '2020-01-01T08:00:00.000Z'),
                        (1000, '2020-01-02T00:00:00.000Z')
                    """);
            drainWalQueue();

            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            Assert.assertTrue("footer must report NOT NULL after conversion", footerNotNull("t", "x"));

            // Control: no ALTER. The O3 update must keep the footer flag.
            execute("INSERT INTO t VALUES (0, '2020-01-01T00:30:00.000Z')");
            drainWalQueue();

            assertQuery("SELECT count() c FROM t WHERE x IS NULL")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            c
                            0
                            """);
            Assert.assertTrue(
                    "footer must keep NOT NULL after a no-ALTER O3 update",
                    footerNotNull("t", "x")
            );
        });
    }

    @Test
    public void testSetNotNullO3UpdateSyncsFooter() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, 1_000_000_000);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (x INT, ts TIMESTAMP NOT NULL)
                    TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            // One genuine NULL lands in the first row group of the file.
            execute("""
                    INSERT INTO t VALUES
                        (1, '2020-01-01T01:00:00.000Z'),
                        (NULL, '2020-01-01T02:00:00.000Z'),
                        (3, '2020-01-01T03:00:00.000Z'),
                        (4, '2020-01-01T04:00:00.000Z'),
                        (5, '2020-01-01T05:00:00.000Z'),
                        (6, '2020-01-01T06:00:00.000Z'),
                        (7, '2020-01-01T07:00:00.000Z'),
                        (8, '2020-01-01T08:00:00.000Z'),
                        (1000, '2020-01-02T00:00:00.000Z')
                    """);
            drainWalQueue();

            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            Assert.assertFalse("footer must report nullable after conversion", footerNotNull("t", "x"));

            // Inverse direction: reclassify in place. The old physical NULL
            // becomes the INT sentinel bit pattern read as data.
            execute("ALTER TABLE t ALTER COLUMN x SET NOT NULL");
            drainWalQueue();
            execute("INSERT INTO t VALUES (42, '2020-01-01T00:30:00.000Z')");
            drainWalQueue();

            // Table-level truth under the new logical schema: no NULLs, the
            // reclassified row surfaces the sentinel bit pattern as data.
            assertQuery("SELECT count() c FROM t WHERE x IS NULL")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            c
                            0
                            """);
            assertQuery("SELECT x::string x_str FROM t WHERE ts = '2020-01-01T02:00:00.000Z'")
                    .noLeakCheck()
                    .returns("""
                            x_str
                            -2147483648
                            """);

            Assert.assertTrue(
                    "footer must report NOT NULL after SET NOT NULL + O3 update",
                    footerNotNull("t", "x")
            );
        });
    }

    private boolean footerNotNull(String tableName, String columnName) {
        boolean found = false;
        boolean notNull = false;
        try (TableReader reader = getReader(tableName)) {
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                if (reader.getPartitionFormat(i) != PartitionFormat.PARQUET) {
                    continue;
                }
                Assert.assertFalse("expected exactly one parquet partition", found);
                found = true;
                reader.openPartition(i);
                try (ParquetFileDecoder footerDecoder = new ParquetFileDecoder()) {
                    footerDecoder.of(
                            reader.getParquetAddr(i),
                            reader.getParquetFileSize(i),
                            MemoryTag.NATIVE_PARQUET_PARTITION_DECODER
                    );
                    final ParquetFileDecoder.Metadata metadata = footerDecoder.metadata();
                    final int columnIndex = metadata.getColumnIndex(columnName);
                    Assert.assertTrue("column " + columnName + " missing from parquet footer", columnIndex >= 0);
                    notNull = metadata.isNotNull(columnIndex);
                }
            }
        }
        Assert.assertTrue("no parquet partition found in " + tableName, found);
        return notNull;
    }
}
